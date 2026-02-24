"""
Kenne Index x OKX 自动定投
用法:
  python3 kenne_dca.py --update           # 仅补全 K 线数据
  python3 kenne_dca.py --notify           # 发送信号邮件（不交易）
  python3 kenne_dca.py --dry-run          # 模拟完整流程（不下单）
  python3 kenne_dca.py                    # 补数据 + 真实下单
  python3 kenne_dca.py --daemon           # 守护进程，按 RUN_INTERVAL_DAYS 间隔执行
  python3 kenne_dca.py --notify-daemon    # 守护进程，仅发邮件不交易
  python3 kenne_dca.py --history [YYYY-MM]

环境变量（敏感信息）:
  OKX_API_KEY / OKX_API_SECRET / OKX_API_PASSPHRASE
  SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASSWORD / EMAIL_TO

环境变量（策略配置）:
  BUDGET_MODE          MONTHLY（月度预算自动分配）或 FIXED（每次固定金额）
  BUDGET_AMOUNT        月度总预算（MONTHLY）或单次固定金额（FIXED），单位 USDT
  RUN_INTERVAL_DAYS    执行间隔天数（7=周投, 1=日投, 30=月投）
  SIMULATED            true=模拟盘（默认）, false=真实交易

USD vs USDT:
  历史 CSV 为 Binance USD 数据，OKX 返回 USDT。
  Kenne Index 是纯比率指标，USDT/USD 偏差 < 0.1%，直接拼接无需换算。
"""

import os, sys, json, hmac, base64, hashlib, time, logging, argparse
import datetime, csv, smtplib
from email.mime.text import MIMEText
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional

import requests

sys.path.insert(0, str(Path(__file__).parent))
from kenne_index import analyze as kenne_analyze


# ─── 配置 ─────────────────────────────────────────────────────────────────────
# 所有策略参数均可通过环境变量覆盖，无需修改代码。

CFG = {
    # OKX API
    'API_KEY':        os.environ.get('OKX_API_KEY',        'YOUR_API_KEY'),
    'API_SECRET':     os.environ.get('OKX_API_SECRET',     'YOUR_API_SECRET'),
    'API_PASSPHRASE': os.environ.get('OKX_API_PASSPHRASE', 'YOUR_PASSPHRASE'),

    # 交易模式: true=模拟盘（安全默认），false=真实交易
    # GitHub Actions 中通过 Secrets 设置 SIMULATED=false 以启用真实交易
    'SIMULATED': os.environ.get('SIMULATED', 'true').lower() != 'false',

    # 预算策略
    # BUDGET_MODE: MONTHLY = 月度预算自动分配（推荐）
    #              FIXED   = 每次执行固定金额，无月度上限
    'BUDGET_MODE':        os.environ.get('BUDGET_MODE', 'MONTHLY').strip().upper(),

    # BUDGET_AMOUNT:
    #   MONTHLY 模式下为月度总预算（如 700 = 每月最多投 700 USDT）
    #   FIXED   模式下为单次投入金额（如 175 = 每次投 175 USDT）
    'BUDGET_AMOUNT':      float(os.environ.get('BUDGET_AMOUNT', '700')),

    # RUN_INTERVAL_DAYS: 执行间隔天数，用于 MONTHLY 模式计算每次应投金额
    #   7  = 每周执行（每月约 4.3 次）
    #   1  = 每天执行（每月约 30 次）
    #   14 = 每两周执行（每月约 2 次）
    # 必须与 cron 表达式保持一致，否则月度预算计算将出错
    'RUN_INTERVAL_DAYS':  int(os.environ.get('RUN_INTERVAL_DAYS', '7')),

    # 数据文件（与脚本同目录）
    'DATA_FILES': {
        'BTC': 'btc_4h_data_2018_to_2025.csv',
        'ETH': 'eth_4h_data_2017_to_2025.csv',
        'SOL': 'sol_4h_data_2020_to_2025.csv',
    },

    # OKX 交易对
    'INST_ID': {
        'BTC': 'BTC-USDT',
        'ETH': 'ETH-USDT',
        'SOL': 'SOL-USDT',
    },

    # 各币种单次权重上限（防止单币过度集中）
    'MAX_WEIGHT':     {'BTC': 0.60, 'ETH': 0.50, 'SOL': 0.50},

    # 低于此金额的单笔订单跳过（OKX 现货最低约 $1）
    'MIN_ORDER_USDT': 5.0,

    # 执行记录文件（MONTHLY 模式依赖此文件追踪月度消费）
    'LOG_FILE':       'dca_log.json',

    # 邮件通知（--notify 模式）
    # Gmail:   smtp.gmail.com:587，需开启「应用专用密码」
    # QQ 邮箱: smtp.qq.com:465，需开启 SMTP 授权码
    # 163:     smtp.163.com:465
    'SMTP_HOST':     os.environ.get('SMTP_HOST',     'smtp.gmail.com'),
    'SMTP_PORT':     int(os.environ.get('SMTP_PORT', '587')),
    'SMTP_USER':     os.environ.get('SMTP_USER',     'your@gmail.com'),
    'SMTP_PASSWORD': os.environ.get('SMTP_PASSWORD', 'your_app_password'),
    'EMAIL_TO':      os.environ.get('EMAIL_TO',      'your@gmail.com'),
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-7s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger('dca')


# ─── OKX 客户端 ───────────────────────────────────────────────────────────────

class OKXClient:
    BASE = 'https://www.okx.com'

    def __init__(self, key, secret, passphrase, simulated):
        self.key, self.secret, self.passphrase = key, secret, passphrase
        self.simulated = simulated
        self.sess = requests.Session()
        self.sess.headers['Content-Type'] = 'application/json'

    @staticmethod
    def _ts():
        n = datetime.datetime.utcnow()
        return n.strftime('%Y-%m-%dT%H:%M:%S.') + f'{n.microsecond//1000:03d}Z'

    def _sign(self, ts, method, path, body=''):
        return base64.b64encode(
            hmac.new(self.secret.encode(), (ts + method + path + body).encode(),
                     hashlib.sha256).digest()
        ).decode()

    def _auth(self, method, path, body=''):
        ts = self._ts()
        h  = {'OK-ACCESS-KEY': self.key, 'OK-ACCESS-SIGN': self._sign(ts, method, path, body),
               'OK-ACCESS-TIMESTAMP': ts, 'OK-ACCESS-PASSPHRASE': self.passphrase}
        if self.simulated:
            h['x-simulated-trading'] = '1'
        return h

    def _get(self, path, params=None, auth=False):
        qs   = ('?' + '&'.join(f'{k}={v}' for k, v in params.items())) if params else ''
        full = path + qs
        kw   = {'headers': self._auth('GET', full)} if auth else {}
        r    = self.sess.get(self.BASE + full, timeout=15, **kw)
        r.raise_for_status()
        return r.json()

    def _post(self, path, body):
        s = json.dumps(body)
        r = self.sess.post(self.BASE + path, headers=self._auth('POST', path, s),
                           data=s, timeout=15)
        r.raise_for_status()
        return r.json()

    def candles(self, inst_id, bar='4H', before=None, after=None, limit=100):
        """
        公开 K 线接口，无需签名。时间戳单位：毫秒，exclusive。
        before=T  ->  返回 ts > T 的 K 线（比 T 更新的数据）
        每条: [ts, O, H, L, C, vol_base, vol_ccy, vol_usdt, confirm]
        confirm: '1'=已收盘  '0'=进行中
        """
        p = {'instId': inst_id, 'bar': bar, 'limit': str(limit)}
        if before is not None: p['before'] = str(before)
        if after  is not None: p['after']  = str(after)
        try:
            resp = self._get('/api/v5/market/history-candles', p)
        except Exception:
            resp = self._get('/api/v5/market/candles', p)
        if resp.get('code') != '0':
            raise RuntimeError(f"candles error: {resp.get('msg', resp)}")
        return resp.get('data', [])

    def balance(self, ccy='USDT'):
        resp = self._get('/api/v5/account/balance', {'ccy': ccy}, auth=True)
        if resp.get('code') != '0':
            raise RuntimeError(f"balance error: {resp}")
        for d in resp['data'][0]['details']:
            if d['ccy'] == ccy:
                return float(d['availBal'])
        return 0.0

    def buy_market_usdt(self, inst_id, usdt):
        """市价买单，sz 单位为 USDT（tgtCcy=quote_ccy）"""
        return self._post('/api/v5/trade/order', {
            'instId': inst_id, 'tdMode': 'cash', 'side': 'buy',
            'ordType': 'market', 'sz': f'{usdt:.4f}', 'tgtCcy': 'quote_ccy',
        })


# ─── 数据更新 ─────────────────────────────────────────────────────────────────

BAR_MS = 4 * 3600 * 1000  # 4H in ms


def _candle_to_row(c):
    """
    OKX K 线 -> CSV 行（兼容 Binance 历史格式）
    c[5]=vol_base -> Volume
    c[7]=vol_usdt -> Quote asset volume
    trades/taker 字段 OKX 不提供，填 0（不影响 AHR999 计算）
    """
    ts  = int(c[0])
    odt = datetime.datetime.fromtimestamp(ts / 1000, tz=datetime.timezone.utc).replace(tzinfo=None)
    cdt = datetime.datetime.fromtimestamp((ts + BAR_MS - 1) / 1000, tz=datetime.timezone.utc).replace(tzinfo=None)
    return [
        odt.strftime('%Y-%m-%d %H:%M:%S.%f'),
        c[1], c[2], c[3], c[4], c[5],
        cdt.strftime('%Y-%m-%d %H:%M:%S') + '.999000',
        c[7], '0', '0', '0', '0',
    ]


class DataUpdater:
    def __init__(self, client):
        self.client = client

    def _last_ts_ms(self, path):
        """返回 CSV 中最后一根完整 K 线的开盘时间 ms。倒序扫描，跳过残缺行。"""
        lines = path.read_text(encoding='utf-8').strip().splitlines()
        if len(lines) < 2:
            return None
        for line in reversed(lines[1:]):   # 跳过 header
            parts = line.split(',')
            if len(parts) < 7:
                continue
            ot = parts[0].strip()
            ct = parts[6].strip()
            if not ot or not ct:
                continue
            for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
                try:
                    dt = datetime.datetime.strptime(ot, fmt)
                    return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
                except ValueError:
                    continue
        return None

    def _clean_tail(self, path):
        """删除末尾 Open time 或 Close time 为空的行（Binance 导出残缺尾行）。"""
        lines   = path.read_text(encoding='utf-8').splitlines()
        cleaned = [lines[0]] + [
            l for l in lines[1:]
            if len(l.split(',')) > 6
            and l.split(',')[0].strip()
            and l.split(',')[6].strip()
        ]
        removed = len(lines) - len(cleaned)
        if removed:
            path.write_text('\n'.join(cleaned) + '\n', encoding='utf-8')
        return removed

    def _fetch_after(self, inst_id, since_ms):
        """
        分页拉取 ts > since_ms 的所有已收盘 4H K 线。
        OKX 返回新->旧，每批 100 根，上限 20 批（约 333 天）。
        返回升序（旧->新）列表。
        """
        collected, cursor = [], since_ms
        for _ in range(20):
            try:
                batch = self.client.candles(inst_id, bar='4H', before=cursor, limit=100)
            except Exception as e:
                log.error(f'candles fetch error: {e}')
                break
            if not batch:
                break
            closed = [c for c in batch if c[8] == '1']
            if closed:
                collected.extend(closed)
            newest = int(batch[0][0])
            if newest <= cursor:
                break
            cursor = newest
            if len(batch) < 100:
                break
            time.sleep(0.15)

        seen, unique = set(), []
        for c in collected:
            if c[0] not in seen:
                seen.add(c[0])
                unique.append(c)
        unique.sort(key=lambda c: int(c[0]))
        return unique

    def update(self, symbol, csv_file):
        """补全指定币种 CSV，返回新增根数。"""
        path = Path(csv_file)
        if not path.exists():
            log.warning(f'[{symbol}] CSV not found: {csv_file}')
            return 0

        removed = self._clean_tail(path)
        if removed:
            log.info(f'[{symbol}] removed {removed} incomplete tail rows')

        last_ms = self._last_ts_ms(path)
        if last_ms is None:
            log.warning(f'[{symbol}] no valid data in CSV')
            return 0

        gap     = (int(time.time() * 1000) - last_ms) // BAR_MS
        last_dt = datetime.datetime.fromtimestamp(
            last_ms / 1000, tz=datetime.timezone.utc).replace(tzinfo=None)
        log.info(f'[{symbol}] local last: {last_dt.strftime("%Y-%m-%d %H:%M")} UTC  gap: ~{gap} bars')

        if gap < 1:
            log.info(f'[{symbol}] up to date')
            return 0

        new = [c for c in self._fetch_after(CFG['INST_ID'][symbol], since_ms=last_ms)
               if int(c[0]) > last_ms]

        if not new:
            log.info(f'[{symbol}] current bar not yet closed, no new data')
            return 0

        with open(path, 'a', newline='', encoding='utf-8') as f:
            csv.writer(f).writerows(_candle_to_row(c) for c in new)

        t0 = datetime.datetime.fromtimestamp(
            int(new[0][0]) / 1000, tz=datetime.timezone.utc).replace(tzinfo=None)
        t1 = datetime.datetime.fromtimestamp(
            int(new[-1][0]) / 1000, tz=datetime.timezone.utc).replace(tzinfo=None)
        log.info(f'[{symbol}] +{len(new)} bars  '
                 f'{t0.strftime("%Y-%m-%d %H:%M")} -> {t1.strftime("%Y-%m-%d %H:%M")} UTC')
        return len(new)


# ─── 预算追踪 ─────────────────────────────────────────────────────────────────

@dataclass
class Record:
    ts:       str
    symbol:   str
    inst_id:  str
    usdt:     float
    kenne_index: float
    mult:     float
    momentum: str
    order_id: str
    status:   str    # filled | dry_run | skipped | failed
    note:     str = ''


class Budget:
    """
    预算管理器，支持两种模式：

    MONTHLY 模式（推荐）:
      - BUDGET_AMOUNT = 月度总上限（如 700）
      - 每次可用额 = min(月均单次, 月剩余 / 月剩余执行次数)
      - 月均单次 = BUDGET_AMOUNT / (30 / RUN_INTERVAL_DAYS)
      - 自动防止某月超支；月初重置

    FIXED 模式:
      - BUDGET_AMOUNT = 每次固定投入（如 175）
      - 不做月度追踪，每次直接使用该金额
      - 适合已在外部控制总投入、只需信号驱动的场景
    """

    def __init__(self):
        self.path         = Path(CFG['LOG_FILE'])
        self.mode         = CFG['BUDGET_MODE']          # 'MONTHLY' | 'FIXED'
        self.amount       = CFG['BUDGET_AMOUNT']        # 月预算 或 单次固定额
        self.interval     = CFG['RUN_INTERVAL_DAYS']    # 执行间隔天数
        self.recs         = json.loads(self.path.read_text()) if self.path.exists() else []

        # 每月预计执行次数（MONTHLY 模式使用）
        # 用 30 天而非实际天数，保证计算稳定；月末多余额度自动结转
        self._runs_per_month = 30.0 / self.interval

    def _save(self):
        self.path.write_text(json.dumps(self.recs, indent=2, ensure_ascii=False))

    def _month_str(self):
        return datetime.date.today().strftime('%Y-%m')

    def spent_this_month(self):
        """当月已实际执行（filled/dry_run）的总金额。"""
        m = self._month_str()
        return sum(
            r['usdt'] for r in self.recs
            if r.get('ts', '').startswith(m)
            and r.get('status') in ('filled', 'dry_run')
        )

    def monthly_remaining(self):
        """当月剩余可用额（仅 MONTHLY 模式有意义）。"""
        return max(0.0, self.amount - self.spent_this_month())

    def this_run_amount(self):
        """
        计算本次应投入的金额。

        FIXED 模式: 直接返回 BUDGET_AMOUNT。

        MONTHLY 模式:
          目标单次金额 = BUDGET_AMOUNT / runs_per_month
          月剩余次数   = 本月剩余天数 / RUN_INTERVAL_DAYS（最少 1）
          本次金额     = min(目标单次, 月剩余 / 月剩余次数)
          → 月初按均匀节奏投，月末如有结余则自动补足，永不超出月度上限
        """
        if self.mode == 'FIXED':
            return self.amount

        # MONTHLY 模式
        target_per_run = self.amount / self._runs_per_month
        remaining      = self.monthly_remaining()
        if remaining <= 0:
            return 0.0

        # 本月剩余天数（至少算 1 天，避免月末最后一天除以 0）
        today  = datetime.date.today()
        next_m = datetime.date(
            today.year + (today.month == 12),
            today.month % 12 + 1, 1
        )
        days_left = max(1, (next_m - today).days)
        runs_left = max(1, round(days_left / self.interval))

        return min(target_per_run, remaining / runs_left)

    def add(self, r):
        self.recs.append(asdict(r))
        self._save()

    def summary_str(self):
        """返回当前预算状态的单行描述，用于日志和邮件。"""
        if self.mode == 'FIXED':
            return (f'mode=FIXED  per_run=${self.amount:.0f}'
                    f'  this_month_spent=${self.spent_this_month():.2f}')
        return (f'mode=MONTHLY  budget=${self.amount:.0f}/mo'
                f'  spent=${self.spent_this_month():.2f}'
                f'  remaining=${self.monthly_remaining():.2f}'
                f'  interval={self.interval}d')


# ─── 资金分配 ─────────────────────────────────────────────────────────────────

def allocate(signals, budget_usdt):
    """
    按 final_mult 权重分配预算，并应用各币种权重上限（3 轮迭代保证收敛）。
    budget_usdt: 本次可用总额（由 Budget.this_run_amount() 提供）。
    """
    active = [s for s in signals if s['final_mult'] > 0]
    if not active or budget_usdt <= 0:
        return []

    norm = {s['symbol']: s['final_mult'] for s in active}
    for _ in range(3):
        total = sum(norm.values())
        norm  = {k: v / total for k, v in norm.items()}
        cap   = {k: min(v, CFG['MAX_WEIGHT'].get(k, 1.0)) for k, v in norm.items()}
        if sum(cap.values()) == 0:
            break
        norm = cap

    total = sum(norm.values())
    norm  = {k: v / total for k, v in norm.items()}
    return [{**s,
             'usdt_amount': round(norm.get(s['symbol'], 0) * budget_usdt, 2),
             'weight':      round(norm.get(s['symbol'], 0), 4)}
            for s in active]


# ─── 主流程 ───────────────────────────────────────────────────────────────────

def _make_client():
    return OKXClient(CFG['API_KEY'], CFG['API_SECRET'],
                     CFG['API_PASSPHRASE'], CFG['SIMULATED'])


def run_update():
    updater = DataUpdater(_make_client())
    total   = 0
    for sym, f in CFG['DATA_FILES'].items():
        try:
            total += updater.update(sym, f)
        except Exception as e:
            log.error(f'[{sym}] update failed: {e}')
    log.info(f'update done, {total} new bars total')


def run_dca(dry_run=False):
    log.info(f'--- Kenne Index DCA {"[dry-run]" if dry_run else "[live]"} ---')

    client  = _make_client()
    updater = DataUpdater(client)
    budget  = Budget()

    # 1. 补全 K 线
    log.info('[1/4] updating market data')
    total_new = 0
    for sym, f in CFG['DATA_FILES'].items():
        try:
            total_new += updater.update(sym, f)
        except Exception as e:
            log.error(f'[{sym}] update failed, using local data: {e}')
    log.info(f'{total_new} new bars added' if total_new else 'data up to date')

    # 2. 预算检查
    log.info('[2/4] budget check')
    run_amount = budget.this_run_amount()
    log.info(budget.summary_str() + f'  this_run=${run_amount:.2f}')
    if run_amount < CFG['MIN_ORDER_USDT']:
        log.info('budget for this run is below minimum order amount, skipping')
        return

    # 3. 信号计算
    log.info('[3/4] calculating signals')
    signals = []
    for sym, f in CFG['DATA_FILES'].items():
        try:
            r = kenne_analyze(f, sym)
            if r:
                signals.append(r)
                log.info(f'  {sym}: kenne={r["kenne_index"]:.4f}  '
                         f'momentum={r["momentum"]}  mult={r["final_mult"]:.2f}x')
        except Exception as e:
            log.error(f'[{sym}] analysis failed: {e}')
    if not signals:
        log.error('all analysis failed, abort')
        return

    # 4. 分配并下单
    log.info('[4/4] allocating and ordering')
    allocs = allocate(signals, run_amount)
    if not allocs:
        log.info('all assets in hold zone, no orders this run')
        return

    for a in allocs:
        log.info(f'  plan: {a["symbol"]} ${a["usdt_amount"]:.2f}'
                 f'  weight={a["weight"]:.1%}  mult={a["final_mult"]:.2f}x')
    log.info(f'  total: ${sum(a["usdt_amount"] for a in allocs):.2f}')

    if not dry_run:
        try:
            avail = client.balance('USDT')
            log.info(f'OKX USDT balance: ${avail:.2f}')
            total = sum(a['usdt_amount'] for a in allocs)
            if avail < total * 0.95:
                ratio  = avail * 0.95 / total
                allocs = [{**a, 'usdt_amount': round(a['usdt_amount'] * ratio, 2)}
                          for a in allocs]
                allocs = [a for a in allocs if a['usdt_amount'] >= CFG['MIN_ORDER_USDT']]
                log.warning(f'insufficient balance, scaled to ${sum(a["usdt_amount"] for a in allocs):.2f}')
        except Exception as e:
            log.error(f'balance check failed: {e}')
            if not CFG['SIMULATED']:
                return

    for a in allocs:
        sym     = a['symbol']
        inst_id = CFG['INST_ID'][sym]
        usdt    = a['usdt_amount']
        ts      = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

        if usdt < CFG['MIN_ORDER_USDT']:
            log.info(f'  {sym}: ${usdt:.2f} below minimum, skip')
            budget.add(Record(ts, sym, inst_id, usdt, a['kenne_index'],
                              a['final_mult'], a['momentum'], '', 'skipped', 'below minimum'))
            continue

        if dry_run:
            log.info(f'  {sym}: [dry-run] would buy ${usdt:.2f} USDT')
            budget.add(Record(ts, sym, inst_id, usdt, a['kenne_index'],
                              a['final_mult'], a['momentum'], 'DRY_RUN', 'dry_run'))
            continue

        try:
            resp = client.buy_market_usdt(inst_id, usdt)
            if resp.get('code') == '0':
                oid = resp['data'][0]['ordId']
                log.info(f'  {sym}: filled  order={oid}  ${usdt:.2f} USDT')
                budget.add(Record(ts, sym, inst_id, usdt, a['kenne_index'],
                                  a['final_mult'], a['momentum'], oid, 'filled'))
            else:
                err = resp.get('data', [{}])[0].get('sMsg', str(resp))
                log.error(f'  {sym}: order failed: {err}')
                budget.add(Record(ts, sym, inst_id, usdt, a['kenne_index'],
                                  a['final_mult'], a['momentum'], '', 'failed', err))
        except Exception as e:
            log.error(f'  {sym}: exception: {e}')
            budget.add(Record(ts, sym, inst_id, usdt, a['kenne_index'],
                              a['final_mult'], a['momentum'], '', 'failed', str(e)))
        time.sleep(0.3)

    log.info(budget.summary_str())
    log.info('--- done ---')


# ─── 守护进程 ─────────────────────────────────────────────────────────────────

def _next_run_time():
    """
    计算下次执行时间：今天或之后最近的 09:00，间隔 >= RUN_INTERVAL_DAYS 天。
    守护进程使用，GitHub Actions 场景下不需要此函数。
    """
    now      = datetime.datetime.now()
    interval = datetime.timedelta(days=CFG['RUN_INTERVAL_DAYS'])
    target   = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if target <= now:
        target += interval
    return target


def run_daemon(dry_run=False):
    log.info(f'daemon started, interval={CFG["RUN_INTERVAL_DAYS"]}d, next run at 09:00 (Ctrl+C to stop)')
    while True:
        t    = _next_run_time()
        secs = (t - datetime.datetime.now()).total_seconds()
        log.info(f'next run: {t.strftime("%Y-%m-%d %H:%M")} ({secs/3600:.1f}h from now)')
        time.sleep(secs)
        try:
            run_dca(dry_run=dry_run)
        except Exception as e:
            log.error(f'run_dca exception: {e}')
        time.sleep(60)


# ─── 历史记录 ─────────────────────────────────────────────────────────────────

def show_history(month=None):
    recs = json.loads(Path(CFG['LOG_FILE']).read_text()) \
           if Path(CFG['LOG_FILE']).exists() else []
    if month:
        recs = [r for r in recs if r.get('ts', '').startswith(month)]
    if not recs:
        print('no records')
        return

    status_map = {'filled': 'OK', 'dry_run': 'DRY', 'failed': 'ERR', 'skipped': 'SKP'}
    total = 0.0
    print(f'\n{"date":<20} {"sym":<4} {"usdt":>8} {"kenne":>8} {"mult":>6} status')
    print('-' * 58)
    for r in recs:
        u  = r.get('usdt', 0)
        if r['status'] in ('filled', 'dry_run'):
            total += u
        st = status_map.get(r['status'], '???')
        print(f'  {r.get("ts",""):<18} {r["symbol"]:<4} '
              f'{u:>8.2f} {r.get("kenne_index",0):>8.4f} '
              f'{r.get("mult",0):>5.2f}x  {st}')
    print(f'{"-"*58}\n  total: ${total:.2f}')


# ─── 邮件通知 ─────────────────────────────────────────────────────────────────

def _send_email(subject, body):
    """端口 465: SSL（QQ/163）；端口 587: STARTTLS（Gmail/Outlook）。"""
    msg            = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From']    = CFG['SMTP_USER']
    msg['To']      = CFG['EMAIL_TO']

    host, port = CFG['SMTP_HOST'], CFG['SMTP_PORT']
    try:
        if port == 465:
            with smtplib.SMTP_SSL(host, port, timeout=15) as s:
                s.login(CFG['SMTP_USER'], CFG['SMTP_PASSWORD'])
                s.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=15) as s:
                s.ehlo()
                s.starttls()
                s.login(CFG['SMTP_USER'], CFG['SMTP_PASSWORD'])
                s.send_message(msg)
        log.info(f'email sent -> {CFG["EMAIL_TO"]}')
    except Exception as e:
        log.error(f'email failed: {e}')


def _build_report(signals, allocs, budget):
    """构建信号报告正文（内容随 BUDGET_MODE / BUDGET_AMOUNT / RUN_INTERVAL_DAYS 动态变化）。"""
    date = datetime.date.today().strftime('%Y-%m-%d')

    # 标题行（根据模式动态生成）
    if budget.mode == 'FIXED':
        budget_desc = f'每次固定 ${budget.amount:.0f} USDT'
    else:
        interval_label = {1: '日投', 7: '周投', 14: '双周投', 30: '月投'}.get(
            budget.interval, f'每{budget.interval}天投')
        budget_desc = f'${budget.amount:.0f}/月  {interval_label}'

    lines = [
        f'Kenne Index 定投信号  {date}',
        f'策略: {budget_desc}',
        '=' * 46,
        '',
        '当前信号',
        '-' * 46,
    ]

    for s in signals:
        ahr = s['kenne_index']
        prc = s.get('price', 0)
        if   ahr < 0.45:            zone = '极低估'
        elif s['base_mult'] > 0:    zone = '定投区'
        else:                       zone = '观望区'
        lines.append(
            f"  {s['symbol']:<4}  价格 {prc:>10,.2f} USDT  "
            f"Kenne {ahr:.4f}  {zone}  {s['momentum']}  建议 {s['final_mult']:.2f}x"
        )

    # 本次分配
    run_amount = budget.this_run_amount()
    lines += ['', f'本次分配建议（${run_amount:.2f} USDT）', '-' * 46]
    if allocs:
        for a in allocs:
            lines.append(
                f"  {a['symbol']:<4}  ${a['usdt_amount']:>7.2f} USDT"
                f"  权重 {a['weight']:.0%}  倍数 {a['final_mult']:.2f}x"
            )
        lines.append(f"  {'合计':<4}  ${sum(a['usdt_amount'] for a in allocs):>7.2f} USDT")
    else:
        lines.append('  当前无买入信号，本次停止定投')

    # 预算汇总（MONTHLY 模式显示月度进度）
    lines += ['']
    if budget.mode == 'MONTHLY':
        lines.append(
            f'月预算 ${budget.amount:.0f}  '
            f'已花 ${budget.spent_this_month():.2f}  '
            f'剩余 ${budget.monthly_remaining():.2f}'
        )
    else:
        lines.append(f'本月已投 ${budget.spent_this_month():.2f}（FIXED 模式，无月度上限）')

    lines += [
        '',
        '-' * 46,
        '本邮件由 Kenne Index 定投系统自动生成，仅供参考，不构成投资建议',
    ]
    return '\n'.join(lines)


def run_notify():
    """更新数据 -> 计算信号 -> 发送邮件。不执行任何交易。"""
    log.info('--- Kenne Index notify ---')

    client  = _make_client()
    updater = DataUpdater(client)
    budget  = Budget()

    log.info('[1/3] updating market data')
    for sym, f in CFG['DATA_FILES'].items():
        try:
            updater.update(sym, f)
        except Exception as e:
            log.error(f'[{sym}] update failed, using local data: {e}')

    log.info('[2/3] calculating signals')
    signals = []
    for sym, f in CFG['DATA_FILES'].items():
        try:
            r = kenne_analyze(f, sym)
            if r:
                signals.append(r)
                log.info(f'  {sym}: kenne={r["kenne_index"]:.4f}  '
                         f'momentum={r["momentum"]}  mult={r["final_mult"]:.2f}x')
        except Exception as e:
            log.error(f'[{sym}] analysis failed: {e}')

    if not signals:
        log.error('all analysis failed')
        return

    allocs = allocate(signals, budget.this_run_amount())

    log.info('[3/3] sending email')
    log.info(budget.summary_str())
    subject = f'Kenne Index 定投信号 {datetime.date.today().strftime("%Y-%m-%d")}'
    _send_email(subject, _build_report(signals, allocs, budget))
    log.info('--- done ---')


def run_notify_daemon():
    """守护进程：按 RUN_INTERVAL_DAYS 间隔自动发送信号邮件。"""
    log.info(f'notify daemon started, interval={CFG["RUN_INTERVAL_DAYS"]}d (Ctrl+C to stop)')
    while True:
        t    = _next_run_time()
        secs = (t - datetime.datetime.now()).total_seconds()
        log.info(f'next notify: {t.strftime("%Y-%m-%d %H:%M")} ({secs/3600:.1f}h from now)')
        time.sleep(secs)
        try:
            run_notify()
        except Exception as e:
            log.error(f'run_notify exception: {e}')
        time.sleep(60)


# ─── 入口 ─────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description='Kenne Index x OKX auto DCA')
    p.add_argument('--update',        action='store_true', help='update CSV data only')
    p.add_argument('--notify',        action='store_true', help='send signal email (no trade)')
    p.add_argument('--dry-run',       action='store_true', help='simulate without ordering')
    p.add_argument('--daemon',        action='store_true', help='run on interval, real orders')
    p.add_argument('--notify-daemon', action='store_true', help='run on interval, email only')
    p.add_argument('--history',       nargs='?', const='', metavar='YYYY-MM', help='show history')
    a = p.parse_args()

    if   a.history is not None: show_history(month=a.history or None)
    elif a.update:               run_update()
    elif a.notify:               run_notify()
    elif a.notify_daemon:        run_notify_daemon()
    elif a.daemon:               run_daemon(dry_run=a.dry_run)
    else:                        run_dca(dry_run=a.dry_run)


if __name__ == '__main__':
    main()
