# Running the order book recorder 24/7 on Oracle Cloud (free)

Runs `scripts/orderbook_trade_recorder.py` for CoinDCX on an Oracle Cloud **Always Free** VM in Docker, and pulls the
recorded files to your laptop with `sync_market_data.ps1`.

- **Cost:** ₹0, as long as you only create resources marked *Always Free-eligible*.
- **No API keys on the server.** It uses `coindcx_paper_trade`, which streams the same public order book and trades as
  `coindcx`, and the files still land in `data/market_data/coindcx/<PAIR>/`.
- **Data size:** roughly 5–8 MB per pair per day, so the free 47 GB disk lasts years.

Files involved:

| File | Purpose |
|---|---|
| `docker-compose.recorder.yml` | Runs the recorder headless, restarts it after crashes and reboots |
| `conf/scripts/conf_recorder_server.yml` | Recorder config for the server (`coindcx_paper_trade`, pairs, depth) |
| `conf/conf_client.yml` | Must list `coindcx` under `paper_trade_exchanges` |
| `sync_market_data.ps1` | Run on your laptop to download new data |

---

## 0. Test the server config on your laptop first (5 min)

Stop the normal recorder, so two recorders don't write to the same files, then run the server config for a minute:

```powershell
.\run_recorder.ps1 -Stop
$env:CONFIG_PASSWORD = "<your hummingbot password>"
.\run_recorder.ps1 -Config conf_recorder_server.yml
# wait ~1 min, then check that data/market_data/coindcx/BTC-USDT/ files are growing
Get-ChildItem data\market_data\coindcx\BTC-USDT | Sort-Object LastWriteTime | Select-Object -Last 2
.\run_recorder.ps1 -Config conf_recorder_server.yml -Stop
```

If both `_book` and `_trades` files grew, the paper trade connector works. Continue below.

## 1. Create the Oracle Cloud account

1. Sign up at <https://www.oracle.com/cloud/free/>.
2. **Home Region: pick `India West (Mumbai)` or `India South (Hyderabad)`.** It cannot be changed later, and Always Free
   VMs only run in your home region. India is also closest to CoinDCX.
3. Card verification places a small temporary hold. You are not charged.

## 2. Create the VM

**Menu → Compute → Instances → Create instance**

1. **Image:** *Change image* → **Canonical Ubuntu 24.04**.
2. **Shape:** *Change shape* → **Ampere** → `VM.Standard.A1.Flex` → **2 OCPU, 12 GB memory**.
   It must show the *Always Free-eligible* tag. The free allowance is 4 OCPU / 24 GB in total.
3. **Networking:** keep the defaults, with *Assign a public IPv4 address* checked.
4. **SSH keys:** *Generate a key pair for me* → **Download private key**. Save it as
   `C:\Users\dipti\.ssh\oracle_recorder.key`.
5. Click **Create**. Once it says *Running*, copy the **Public IP address**.

> **"Out of capacity" error?** Free ARM capacity in India is often full. Retry a few hours later, try another
> *Availability domain*, or use 1 OCPU / 6 GB. As a last resort, use `VM.Standard.E2.1.Micro` (1 GB RAM, also free).
> It works, but the Docker build in step 5 is slow there, so add the swap in step 3 first.

## 3. Connect and install Docker

On your laptop (PowerShell). Restrict the key file's permissions first, because ssh refuses keys that other users can read:

```powershell
icacls $HOME\.ssh\oracle_recorder.key /inheritance:r /grant:r "$($env:USERNAME):R"
ssh -i $HOME\.ssh\oracle_recorder.key ubuntu@<public ip>
```

On the server:

```bash
# Docker (the service starts automatically on boot)
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker ubuntu

# 4 GB swap so the image build never runs out of memory
sudo fallocate -l 4G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

exit   # log out and back in so the docker group applies
```

## 4. Get the code onto the server

First commit and push this branch from your laptop (it must contain `docker-compose.recorder.yml` and the recorder fixes).
Then on the server:

```bash
git clone -b dipti-hm/backtesting https://github.com/Haveli-Makers/hummingbot.git
```

The repo is private, so git asks for a username and password. Enter your GitHub username, and as the password a
**fine-grained personal access token** with read-only *Contents* access to `Haveli-Makers/hummingbot`
(GitHub → Settings → Developer settings → Personal access tokens).

Then, **from your laptop** (PowerShell, in the `hummingbot` folder), copy the files that git doesn't track. Configs are
gitignored, and `.password_verification` lets the server check your Hummingbot password:

```powershell
$key = "$HOME\.ssh\oracle_recorder.key"; $srv = "ubuntu@<public ip>"
scp -i $key conf\.password_verification conf\conf_client.yml "${srv}:~/hummingbot/conf/"
scp -i $key conf\scripts\conf_recorder_server.yml "${srv}:~/hummingbot/conf/scripts/"
```

None of these contain exchange API keys.

## 5. Start the recorder

On the server:

```bash
cd ~/hummingbot
printf 'CONFIG_PASSWORD=%s\nRECORDER_CONFIG=conf_recorder_server.yml\n' '<your hummingbot password>' > .env
chmod 600 .env
docker compose -f docker-compose.recorder.yml up -d --build    # first build takes ~15–30 min
```

Check it's recording:

```bash
docker compose -f docker-compose.recorder.yml logs --tail 20
ls -la data/market_data/coindcx/BTC-USDT/
```

`restart: unless-stopped` brings it back after crashes and after the VM reboots. You can now close the SSH window, and
your laptop can sleep.

## 6. Download the data to your laptop

```powershell
$env:RECORDER_SERVER = "ubuntu@<public ip>"
$env:RECORDER_KEY = "$HOME\.ssh\oracle_recorder.key"
.\sync_market_data.ps1
```

This only downloads new or grown files. Run it before each backtest.

**Don't run the laptop recorder for the same pair at the same time.** Sync overwrites a local file with the server's
copy when their sizes differ.

---

## Day-to-day

| Task | Command (on the server, in `~/hummingbot`) |
|---|---|
| Watch logs | `docker compose -f docker-compose.recorder.yml logs -f` |
| Stop / start | `docker compose -f docker-compose.recorder.yml down` / `up -d` |
| Record more pairs | edit `trading_pairs` in `conf/scripts/conf_recorder_server.yml` (e.g. `BTC-USDT,ETH-USDT`), then `docker compose -f docker-compose.recorder.yml restart` |
| Update code | `git pull && docker compose -f docker-compose.recorder.yml up -d --build` |
| Disk usage | `du -sh data/market_data; df -h /` |

## Keeping it free and running

- **Idle reclamation.** Oracle may stop Always Free VMs that look idle for 7 days (CPU, network and memory all under
  ~20%). The recorder is light, so it could get flagged. Two ways to avoid it:
  - Check the VM in the console every few days. A stopped VM can simply be started again, and the data on disk is kept.
  - Or upgrade the account to **Pay As You Go** (Billing → Upgrade). Always Free resources stay free, and reclamation no
    longer applies. Add a **Budget** alert at ₹100 (Billing → Budgets) so any accidental paid resource emails you.
    Check Oracle's current Always Free terms before deciding.
- Only create things tagged *Always Free-eligible*. The VM, its boot volume (≤200 GB total) and 10 TB/month of outbound
  traffic are covered.
- Security: only SSH (port 22) is open by default, and the recorder needs no inbound ports. Keep the private key safe.
