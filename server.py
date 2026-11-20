#!/usr/bin/env python3
"""Radar Cripto - servidor local.

Serve o dashboard e expõe dois endpoints somente-leitura: /api/portfolio (saldos)
e /api/cost (preço médio pelo histórico de trades spot e Convert). A chave fica no arquivo .env e nunca é
enviada ao navegador. O servidor só escuta em 127.0.0.1 e não possui nenhum
código capaz de criar ordens ou saques.
"""
import hashlib
import hmac
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.abspath(__file__))
PORT = 8765
BINANCE = "https://api.binance.com"
PUBLIC_FILES = {"/", "/index.html"}


def load_env():
    env = {}
    try:
        with open(os.path.join(ROOT, ".env"), encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip("'\"")
    except FileNotFoundError:
        pass
    return env


def signed_get(path, params, key, secret):
    params = dict(params, timestamp=int(time.time() * 1000), recvWindow=10000)
    query = urllib.parse.urlencode(params)
    sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    req = urllib.request.Request(
        f"{BINANCE}{path}?{query}&signature={sig}", headers={"X-MBX-APIKEY": key}
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.load(r)


def binance_error(e):
    if isinstance(e, urllib.error.HTTPError):
        try:
            msg = json.load(e).get("msg", "")
        except Exception:
            msg = ""
        return f"Binance HTTP {e.code}: {msg}"
    return f"Falha de rede: {type(e).__name__}"


PUBLIC = "https://data-api.binance.vision"
_price_cache, _convert_cache = {}, {"at": 0.0, "rows": []}
CONVERT_MONTHS, CONVERT_TTL = 24, 900
_convert_lock = threading.Lock()


def public_get(path):
    with urllib.request.urlopen(PUBLIC + path, timeout=15) as r:
        return json.load(r)


def price_at(pair, ms):
    """Fechamento do candle de 1 minuto de <pair> no instante ms; None se par não existe."""
    k = (pair, ms // 60000)
    if k not in _price_cache:
        try:
            rows = public_get(f"/api/v3/klines?symbol={pair}&interval=1m&startTime={k[1] * 60000}&limit=1")
            _price_cache[k] = float(rows[0][4]) if rows else None
        except Exception:
            _price_cache[k] = None
    return _price_cache[k]


def usdt_value(asset, amount, ms):
    """Valor em USDT de <amount> de <asset> no instante ms; None se sem preço."""
    if asset == "USDT":
        return amount
    p = price_at(asset + "USDT", ms)
    if p:
        return amount * p
    p = price_at("USDT" + asset, ms)  # moedas fiduciárias: USDTBRL, USDTTRY...
    return amount / p if p else None


def convert_history(key, secret):
    """Conversões (Binance Convert) concluídas nos últimos CONVERT_MONTHS meses, com cache."""
    # Lock: pedidos simultâneos esperam a primeira busca em vez de repetir 24 chamadas cada
    with _convert_lock:
        if time.time() - _convert_cache["at"] < CONVERT_TTL:
            return _convert_cache["rows"]
        rows = _fetch_convert_history(key, secret)
        _convert_cache.update(at=time.time(), rows=rows)
        return rows


def _fetch_convert_history(key, secret):
    now, window = int(time.time() * 1000), 30 * 86400000

    def fetch(i):  # API aceita janelas de no máximo 30 dias
        params = {"startTime": now - (i + 1) * window, "endTime": now - i * window, "limit": 1000}
        for attempt in (1, 2):
            try:
                return signed_get("/sapi/v1/convert/tradeFlow", params, key, secret).get("list", [])
            except urllib.error.URLError as e:
                if attempt == 2 or isinstance(e, urllib.error.HTTPError):
                    raise
                time.sleep(1)

    with ThreadPoolExecutor(max_workers=4) as pool:
        pages = list(pool.map(fetch, range(CONVERT_MONTHS)))
    rows = [t for page in pages for t in page if t.get("orderStatus") == "SUCCESS"]
    return rows


def avg_cost(symbol):
    """Preço médio da posição atual em <symbol>, pelo histórico de trades spot.

    Método custo médio: compra soma quantidade e custo; venda reduz ambos
    proporcionalmente (não altera o preço médio). Taxas entram no custo.
    """
    env = load_env()
    key, secret = env.get("BINANCE_API_KEY", ""), env.get("BINANCE_API_SECRET", "")
    if not key or not secret:
        return 200, {"configured": False}
    base, trades, from_id = symbol[:-4], [], 0
    try:
        for _ in range(10):  # até 10 mil trades
            page = signed_get(
                "/api/v3/myTrades", {"symbol": symbol, "limit": 1000, "fromId": from_id}, key, secret
            )
            trades += page
            if len(page) < 1000:
                break
            from_id = page[-1]["id"] + 1
    except Exception as e:
        return 502, {"configured": True, "error": binance_error(e)}

    # Eventos (instante, quantidade com sinal, custo em USDT para compras)
    events = []
    for t in trades:
        q, quote = float(t["qty"]), float(t["quoteQty"])
        fee, fee_asset = float(t["commission"]), t["commissionAsset"]
        if t["isBuyer"]:
            events.append((t["time"], q - (fee if fee_asset == base else 0.0),
                           quote + (fee if fee_asset == "USDT" else 0.0)))
        else:
            events.append((t["time"], -(q + (fee if fee_asset == base else 0.0)), 0.0))

    converts, convert_error = 0, None
    try:
        for c in convert_history(key, secret):
            ms = c["createTime"]
            if c["toAsset"] == base:  # comprou via Convert: custo = valor em USDT do que foi entregue
                got = float(c["toAmount"])
                paid = usdt_value(c["fromAsset"], float(c["fromAmount"]), ms)
                if paid is None:
                    paid = usdt_value(base, got, ms)
                if paid is not None:
                    events.append((ms, got, paid))
                    converts += 1
            elif c["fromAsset"] == base:  # converteu para outro ativo: conta como venda
                events.append((ms, -float(c["fromAmount"]), 0.0))
                converts += 1
    except Exception as e:
        convert_error = binance_error(e)

    qty = cost = 0.0
    last_buy = None
    for ms, dq, paid in sorted(events):
        if dq > 0:
            qty += dq
            cost += paid
            last_buy = ms
        else:
            if qty > 0:
                cost -= cost / qty * min(-dq, qty)
            qty = max(qty + dq, 0.0)
            if qty < 1e-12:
                qty = cost = 0.0
    return 200, {
        "configured": True,
        "avgCost": cost / qty if qty > 0 else None,
        "trackedQty": qty,
        "trades": len(trades),
        "converts": converts,
        "convertError": convert_error,
        "lastBuy": last_buy,
    }


def portfolio():
    env = load_env()  # relido a cada chamada: editar .env não exige reiniciar
    key, secret = env.get("BINANCE_API_KEY", ""), env.get("BINANCE_API_SECRET", "")
    if not key or not secret:
        return 200, {"configured": False}
    try:
        acct = signed_get("/api/v3/account", {"omitZeroBalances": "true"}, key, secret)
    except Exception as e:
        return 502, {"configured": True, "error": binance_error(e)}

    totals, free = {}, {}
    for b in acct.get("balances", []):
        qty = float(b["free"]) + float(b["locked"])
        asset = b["asset"]
        if not asset.startswith("LD"):
            free[asset] = float(b["free"])  # livre em spot: o que dá para vender agora
        # Saldos em Earn flexível aparecem como LD<ativo> na conta spot
        if asset.startswith("LD") and len(asset) > 3:
            asset = asset[2:]
        if qty > 0:
            totals[asset] = totals.get(asset, 0.0) + qty
    # Permissões desta chave API (não da conta): ideal é só leitura
    can_trade = can_withdraw = None
    try:
        r = signed_get("/sapi/v1/account/apiRestrictions", {}, key, secret)
        can_withdraw = bool(r.get("enableWithdrawals"))
        can_trade = any(
            bool(v) for k, v in r.items()
            if k.startswith("enable") and k not in ("enableReading", "enableWithdrawals")
        )
    except Exception:
        pass  # não conseguiu verificar; frontend mostra "não verificado"
    return 200, {
        "configured": True,
        "balances": [{"asset": a, "qty": q, "free": free.get(a, 0.0)} for a, q in sorted(totals.items())],
        "keyCanTrade": can_trade,
        "keyCanWithdraw": can_withdraw,
    }


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=ROOT, **kw)

    def do_GET(self):
        path, _, query = self.path.partition("?")
        if path in ("/api/portfolio", "/api/cost"):
            # Bloqueia páginas de outros sites tentando ler dados da conta via navegador
            host = self.headers.get("Host", "")
            if host.split(":")[0] not in ("localhost", "127.0.0.1") or self.headers.get("Origin"):
                return self.send_error(403)
            if path == "/api/portfolio":
                status, body = portfolio()
            else:
                symbol = urllib.parse.parse_qs(query).get("symbol", [""])[0]
                if not re.fullmatch(r"[A-Z0-9]{1,12}USDT", symbol):
                    return self.send_error(400)
                status, body = avg_cost(symbol)
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        elif path in PUBLIC_FILES:  # nunca serve .env, server.py etc.
            self.path = "/index.html"
            super().do_GET()
        else:
            self.send_error(404)

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    print(f"Radar Cripto em http://localhost:{PORT}")
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
