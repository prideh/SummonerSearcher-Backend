# Private Prototype: Completed-Game Pro Comparisons & Feed-Clock Alignment Report (Issue #15)

> **Scope Notice**: Private prototype developed on `feature/pro-benchmark-prototype` for owner review.
> No public endpoints, UI changes, or recurring ingestion jobs are enabled.

## 1. Executive Summary & Viability Verdict

We ingested a fixed sample of **8 completed Worlds 2026 games** (`80` player-game timelines, all on tournament patch `16.20.826.3891`, item metadata matched via Data Dragon `16.22.1`) discovered from the `lolesports.com/api/gql` (`homeEvents`, `eventState: ["completed"]`) BFF persisted query and walked their full timelines via `feed.lolesports.com/livestats/v1/{window,details}/{gameId}` (`65,529` unique sub-second frames across `1,626` 10-second windows).

### Key Findings
1. **Naive Elapsed Feed Time (`τ - T_first_frame`) Is Biased and Vulnerable to Pauses**:
   - Every game begins with a pre-spawn loading phase where `totalGold = 0` and `maxHealth = 0` for **4.347s to 4.965s** (mean **`+4.579s ± 0.218s`**) before champions spawn on Fountain (`0:00.0` in-game clock).
   - More importantly, **1 of the 8 games (`IG vs TES Game 1`, `115660540726095110`) experienced a `105.020s` (`1m 45.02s`) mid-game pause** at `22:21:24.286Z` (`6:23.6` in-game clock). Because `rfc460Timestamp` is wall-clock UTC, reading `10:00` as `T_first_frame + 600s` (`22:24:56Z`) actually samples the game at **`8:10` in-game clock**, undercounting 10-minute stats by **`+14 to +25 CS` (mean `+15.6 CS`)**, **`+356g to +742g` (mean `+585.5g`)**, and **`+1 to +2 champion levels`** across all 10 players.
   - This directly explains why the initial spot-check of `IG vs TES G1` in Issue #13 saw Rookie (Syndra) at `67–70 CS / 2,841–2,931g / Lvl 7` at `22:25:00Z` (`8:14` in-game) when his true `10:00` in-game stats at `22:26:45.665Z` were **`92 CS / 3,567g / Lvl 8`**.
2. **True In-Game Clock Can Be Reconstructed Directly from the Feed Within `±0.50 Seconds`**:
   - **Zero Anchor (`T_spawn` = In-Game `0:00.0`)**: Defined as the first frame where all 10 participants transition from `totalGold == 0` / `maxHealth == 0` to `totalGold >= 500` and `maxHealth > 0`.
   - **Deterministic Passive-Gold Verification (`T_passive_gold`)**: On Season 2026 patches (`16.20`), passive gold begins at `1:05.0` (`65.000s` in-game clock) at `2.04g/s`, so untouched `500g` players hit `502g` (`+2.0g` integer jump) at theoretical in-game clock **`65.980s`**. Across all 8 games, `T_passive_gold - T_spawn` measured **`65.910s` to `65.944s`** (mean **`65.935s`**, SD **`0.011s`**, total spread **`34ms`**), proving `T_spawn` locks onto true in-game `0:00.0` within **`[-0.070s, -0.036s]` (`±0.07s`)**.
   - **Pause Subtraction (`Σ Δ_pause`)**: Pause intervals are explicitly flagged via `gameState == "paused"` in `/window` (and detectable in `/details` via zero-gold freeze $\max_p \Delta\text{Gold}_p == 0$ over $\Delta\tau \ge 1.8\text{s}$ after $t_{\text{spawn}} + 66\text{s}$). Subtracting cumulative pause duration yields a pause-adjusted in-game clock $t_{\text{game}}(\tau) = (\tau - T_{\text{spawn}}) - \sum \Delta_{\text{pause}}$ with an end-to-end error bound of **`±0.50 seconds`** (dominated by the `1–5 Hz` frame cadence).

---

## 2. Clock-Alignment Test Across All 8 Ingested Games

### 2.1 Per-Game Anchor & Pause Measurements

| Game | `esportsGameId` | `T_first_frame` (UTC) | `T_spawn` (`0:00.0` In-Game) | Pre-Spawn Delay (`T_spawn - T_f0`) | `T_passive_gold - T_spawn` (Theory `65.980s`) | Clock Error at `1:06` | 1st Mid / Side / Jg CS (`s` from `T_spawn`) | Verified Pause (`Σ Δ_pause`) | True In-Game Duration |
| :--- | :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **IG vs TES G1** | `115660540726095110` | `22:14:56.128Z` | `22:15:00.645Z` | `+4.517s` | `65.944s` | `-0.036s` | `61.9s` / `69.9s` / `67.9s` | `105.02s` | `31:02 (1862.0s)` |
| **IG vs TES G2** | `115660540726095111` | `23:07:44.283Z` | `23:07:48.759Z` | `+4.476s` | `65.938s` | `-0.042s` | `61.9s` / `69.9s` / `75.9s` | `0.00s` | `47:09 (2828.8s)` |
| **IG vs TES G3** | `115660540726095112` | `00:16:02.183Z` | `00:16:07.148Z` | `+4.965s` | `65.910s` | `-0.070s` | `62.9s` / `70.9s` / `67.9s` | `0.00s` | `29:29 (1769.1s)` |
| **IG vs TES G4** | `115660540726095113` | `02:20:23.637Z` | `02:20:28.226Z` | `+4.589s` | `65.938s` | `-0.042s` | `59.9s` / `68.9s` / `73.9s` | `0.00s` | `30:58 (1858.1s)` |
| **BLG vs HLE G1** | `115660540726095094` | `22:02:50.240Z` | `22:02:55.086Z` | `+4.846s` | `65.930s` | `-0.050s` | `60.9s` / `67.9s` / `69.9s` | `0.00s` | `41:51 (2511.4s)` |
| **BLG vs HLE G2** | `115660540726095095` | `23:05:07.091Z` | `23:05:11.602Z` | `+4.511s` | `65.937s` | `-0.043s` | `59.9s` / `70.9s` / `74.9s` | `0.00s` | `32:44 (1964.5s)` |
| **G2 vs HLE G1** | `115660540726095082` | `23:14:18.238Z` | `23:14:22.585Z` | `+4.347s` | `65.943s` | `-0.037s` | `60.9s` / `68.9s` / `55.9s` | `0.00s` | `28:06 (1685.6s)` |
| **G2 vs HLE G2** | `115660540726095083` | `00:02:39.092Z` | `00:02:43.472Z` | `+4.380s` | `65.939s` | `-0.041s` | `60.9s` / `68.9s` / `70.9s` | `0.00s` | `26:12 (1572.1s)` |

### 2.2 Early CS Curves vs. Known Season 2026 Spawn Rules (`Patch 16.20`)
- **Season 2026 Wave & Camp Spawn Rules**:
  - Lane minions spawn at Nexus at **`0:30.0` (`30.0s`)**; Mid waves meet at `~0:55` (`55s`), first melee minion dies at `~1:00–1:03` (`60–63s`).
  - Side-lane (Top/Bot) waves meet at `~1:00` (`60s`), first melee minion dies at `~1:08–1:11` (`68–71s`).
  - Jungle camps spawn at **`0:55.0` (`55.0s`)**; first small raptor/wolf or buff monster dies between `~56s` (instant smite/Aoe) and `~76s` (full single-target buff clear).
- **Observed Across the 8 Pro Games (Filtered for Ward Clears `< 55s` or `< 20g`)**:
  - **First Mid Minion CS**: **`59.92s – 62.94s`** (mean **`61.18s`** from `T_spawn`).
  - **First Side-Lane Minion CS**: **`67.94s – 70.95s`** (mean **`69.55s`** from `T_spawn`).
  - **First Jungle Monster CS**: **`55.94s – 75.92s`** (mean **`69.67s`** from `T_spawn`).
  - *Caveat on Raw `creepScore` before `0:55`*: `creepScore` in `livestats` includes level-1 trinket/ward kills (which grant 5–10g/30g before minions meet). Filtering by `t >= 55s` and `Δgold >= 20g` cleanly isolates lane minion CS.

### 2.3 VOD Metadata Sanity Check (`lolesports.com/api/gql` `WatchVodVod`)
- The BFF `homeEvents` query provides `vodFirstFrameTime` (broadcast stream start UTC), `vodStartMillis`, and `vodEndMillis` for each game.
- Across the 8 games, `T_spawn - (vodFirstFrameTime + vodStartMillis)` ranges from **`+325.8s` (`5m 26s`) to `+811.0s` (`13m 31s`)** because `vodStartMillis` marks the start of **Broadcast Champ Select / Draft + Walkouts**, not in-game `0:00`.
- Similarly, `(vodFirstFrameTime + vodEndMillis) - T_finished` is **`+69s to +150s`** (post-game replay/nexus explosion broadcast tail). Therefore, VOD timestamps are useful for deep-linking video playback (`vodStartMillis + (T_spawn - vod_start) + t_in_game + Σ Δ_pause`), while `T_spawn` + `Σ Δ_pause` from `livestats` is the exact source of truth for the in-game clock.

### 2.4 Impact of the `105.02s` Mid-Game Pause in `IG vs TES Game 1` (`115660540726095110`) at `10:00`

At `2026-11-03T22:21:24.286Z` (`6:23.6` after `T_spawn`), `IG vs TES G1` transitioned to `gameState: "paused"` until `2026-11-03T22:23:09.306Z` (`105.020s` freeze). Comparing naive `T_first_frame + 10m` (`22:24:56.208Z`, which is only `8:10.5` in-game!) against true pause-adjusted `10:00.0` (`22:26:45.665Z`):

| Participant | Player | Champion (Role) | Naive `10m` (`T_f0 + 600s` = `8:10` In-Game) | True In-Game `10:00` (`T_spawn + 600s + 105.02s`) | Error if Unadjusted (`True - Naive`) |
| :---: | :--- | :--- | :---: | :---: | :---: |
| P1 | **IG TheShy** | Gnar (top) | `70 CS` / `3252g` / `L7` | **`88 CS` / `3965g` / `L8`** | **`+18 CS`, `+713g`, `+1 Lvl`** |
| P2 | **IG Wei** | Qiyana (jungle) | `73 CS` / `3145g` / `L7` | **`87 CS` / `3746g` / `L8`** | **`+14 CS`, `+601g`, `+1 Lvl`** |
| P3 | **IG Rookie** | Syndra (mid) | `67 CS` / `2841g` / `L7` | **`92 CS` / `3567g` / `L8`** | **`+25 CS`, `+726g`, `+1 Lvl`** |
| P4 | **IG JiaQi** | Kaisa (bottom) | `69 CS` / `2755g` / `L6` | **`92 CS` / `3459g` / `L7`** | **`+23 CS`, `+704g`, `+1 Lvl`** |
| P5 | **IG Meiko** | Nautilus (support) | `14 CS` / `2165g` / `L5` | **`17 CS` / `2542g` / `L6`** | **`+3 CS`, `+377g`, `+1 Lvl`** |
| P6 | **TES 369** | Ambessa (top) | `58 CS` / `2506g` / `L7` | **`76 CS` / `3024g` / `L9`** | **`+18 CS`, `+518g`, `+2 Lvl`** |
| P7 | **TES Tian** | Vi (jungle) | `68 CS` / `3349g` / `L7` | **`84 CS` / `3956g` / `L8`** | **`+16 CS`, `+607g`, `+1 Lvl`** |
| P8 | **TES Creme** | Cassiopeia (mid) | `66 CS` / `2944g` / `L7` | **`87 CS` / `3686g` / `L8`** | **`+21 CS`, `+742g`, `+1 Lvl`** |
| P9 | **TES JackeyLove** | Ezreal (bottom) | `71 CS` / `3248g` / `L6` | **`86 CS` / `3759g` / `L7`** | **`+15 CS`, `+511g`, `+1 Lvl`** |
| P10 | **TES Zhuo** | Rell (support) | `14 CS` / `2057g` / `L5` | **`17 CS` / `2413g` / `L6`** | **`+3 CS`, `+356g`, `+1 Lvl`** |

### 2.5 Summary of Clock Model Error Bounds

| Clock Alignment Model | Formula for Wall Timestamp $\tau(T)$ | Unpaused Games Error (`7/8` games) | Paused Games Error (`1/8` games) | Verdict |
| :--- | :--- | :---: | :---: | :--- |
| **1. Naive Elapsed Feed Time** | `T_first_frame + T` | `-4.58s ± 0.22s` (`-1.0 CS / -34g` at 10m) | **`-109.54s`** (`-15.6 CS / -586g` at 10m) | **Unacceptable** (corrupted by loading delay & pauses) |
| **2. Spawn-Anchored Only** | `T_spawn + T` | **`±0.05s` anchor / `±0.50s` frame** | **`-105.02s`** (`-14.5 CS / -550g` at 10m) | **Incomplete** (fixes `0:00` anchor, still vulnerable to pauses) |
| **3. Spawn-Anchored + Pause-Adjusted (Recommended)** | `T_spawn + T + Σ Δ_pause(≤ T)` | **`±0.50s`** | **`±0.50s`** | **Viable for Exact In-Game Clock Baselines** |

---

## 3. Sample Pro Baseline Tables (8 Completed Worlds 2026 Games, Patch `16.20.826.3891`)

All metrics below are computed using **Clock Model 3 (`Spawn-Anchored + Pause-Adjusted In-Game Clock`, `±0.50s`)**.

### 3.1 Aggregate Baselines by Role (`n = 16` Player-Games per Role)

| Role | `n` | Avg `CS@10` (Range) | Avg `Gold@10` (Range) | Avg `Lvl@10` | Avg `CS@14` | Avg `Gold@14` | Avg `CS@15` (Range) | Avg `Gold@15` (Range) | Avg `Lvl@15` | Avg Boots Timing | Avg 1st Legendary Item | Avg 2nd Legendary Item |
| :--- | :-: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **TOP** | 16 | **84.2** (`60–102`) | **3435g** (`2708–4019`) | 8.3 | 118.9 | 4865g | **127.1** (`95–151`) | **5198g** (`3993–6310`) | 11.2 | `13:55 (835.0s)` | `14:00 (839.9s)` | `23:01 (1380.9s)` |
| **JUNGLE** | 16 | **78.9** (`63–93`) | **3748g** (`3094–4566`) | 7.7 | 111.4 | 5333g | **120.2** (`108–134`) | **5759g** (`4897–6653`) | 10.1 | `24:10 (1450.5s)` | `10:48 (647.5s)` | `18:56 (1135.6s)` |
| **MID** | 16 | **94.5** (`71–107`) | **3675g** (`2983–4060`) | 8.6 | 135.4 | 5267g | **147.1** (`121–157`) | **5698g** (`4825–6278`) | 10.9 | `8:48 (527.7s)` | `13:13 (793.2s)` | `20:03 (1203.1s)` |
| **BOTTOM** | 16 | **89.4** (`70–100`) | **3680g** (`3108–4216`) | 7 | 130.2 | 5517g | **142.6** (`119–162`) | **6025g** (`5327–6658`) | 9.2 | `17:13 (1033.0s)` | `12:08 (727.5s)` | `18:38 (1118.5s)` |
| **SUPPORT** | 16 | **15.9** (`11–22`) | **2452g** (`2080–2877`) | 5.8 | 21.9 | 3454g | **22.4** (`15–31`) | **3675g** (`3357–3936`) | 7.2 | `19:32 (1172.0s)` | `14:05 (844.9s)` | `26:38 (1597.6s)` |

### 3.2 Per-Role + Champion Baselines (`57` Unique Role-Champion Pairs Across `80` Observations)

| Role | Champion | `n` | Players | `CS@10` | `Gold@10` | `Lvl@10` | `CS@15` | `Gold@15` | `Lvl@15` | Boots Timing | 1st Completed Item (Timing) | 2nd Completed Item (Timing) |
| :--- | :--- | :-: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- | :---: |
| `top` | **KSante** | 3 | HLE Zeus, IG TheShy | 93 | 3761g | 8.7 | 137.3 | 5697g | 11.7 | `15:12 (911.8s)` | Iceborn Gauntlet (`14:36 (876.1s)`) | Jak'Sho, The Protean (`23:15 (1395.2s)`) |
| `top` | **Ambessa** | 2 | HLE Zeus, TES 369 | 68 | 2866g | 8.5 | 106.5 | 4388g | 11 | `11:47 (706.7s)` | Eclipse (`16:01 (960.7s)`) | Spear of Shojin (`23:36 (1415.8s)`) |
| `top` | **Gnar** | 2 | BLG Bin, IG TheShy | 93 | 3777g | 8 | 144.5 | 5614g | 12 | `7:35 (454.9s)` | Trinity Force (`14:35 (874.9s)`) | Sterak's Gage (`24:05 (1444.8s)`) |
| `top` | **Olaf** | 2 | BLG Bin, G2 BrokenBlade | 89.5 | 3423g | 8 | 135 | 5431g | 11 | `16:28 (988.4s)` | Experimental Hexplate (`13:53 (833.2s)`) | Death's Dance (`23:57 (1437.3s)`) |
| `top` | **Rumble** | 2 | HLE Zeus, TES 369 | 87 | 3419g | 8.5 | 131 | 5156g | 11 | `10:54 (653.9s)` | Liandry's Torment (`12:54 (773.9s)`) | Bloodletter's Curse (`21:24 (1283.9s)`) |
| `top` | **Aatrox** | 1 | TES 369 | 71 | 2879g | 8 | 103 | 4384g | 11 | `—` | Black Cleaver (`12:52 (771.9s)`) | Sterak's Gage (`22:52 (1371.9s)`) |
| `top` | **Camille** | 1 | G2 BrokenBlade | 76 | 3676g | 8 | 108 | 5634g | 10 | `12:57 (777.3s)` | Trinity Force (`12:57 (776.6s)`) | Ravenous Hydra (`20:57 (1256.8s)`) |
| `top` | **Ornn** | 1 | IG TheShy | 76 | 3116g | 8 | 113 | 4475g | 11 | `15:53 (952.9s)` | Kaenic Rookern (`11:53 (712.9s)`) | Randuin's Omen (`26:53 (1612.9s)`) |
| `top` | **Renekton** | 1 | TES 369 | 85 | 3473g | 9 | 134 | 5002g | 12 | `28:52 (1731.5s)` | Stridebreaker (`13:52 (831.9s)`) | Sterak's Gage (`17:52 (1071.9s)`) |
| `top` | **Vayne** | 1 | IG TheShy | 85 | 3558g | 8 | 129 | 5407g | 11 | `9:51 (591.3s)` | Experimental Hexplate (`13:52 (831.9s)`) | Essence Reaver (`23:51 (1431.3s)`) |
| `jungle` | **JarvanIV** | 2 | HLE Kanavi | 86 | 3726g | 8 | 129 | 5766g | 10 | `17:51 (1070.9s)` | Black Cleaver (`10:29 (629.2s)`) | Sundered Sky (`19:56 (1195.8s)`) |
| `jungle` | **LeeSin** | 2 | BLG Xun, HLE Kanavi | 75.5 | 3644g | 7.5 | 116 | 5782g | 10.5 | `19:00 (1139.5s)` | Eclipse (`9:56 (596.4s)`) | Black Cleaver (`18:51 (1131.2s)`) |
| `jungle` | **Pantheon** | 2 | BLG Xun, G2 SkewMond | 77 | 3660g | 8 | 114.5 | 5366g | 10 | `20:31 (1231.1s)` | Black Cleaver (`10:23 (623.4s)`) | Sundered Sky (`20:58 (1258.2s)`) |
| `jungle` | **Qiyana** | 2 | G2 SkewMond, IG Wei | 84 | 3794g | 8 | 125.5 | 6038g | 10 | `25:57 (1556.6s)` | Voltaic Cyclosword (`11:06 (665.5s)`) | Bastionbreaker (`17:31 (1050.9s)`) |
| `jungle` | **Skarner** | 2 | HLE Kanavi, TES Tian | 76.5 | 3678g | 7.5 | 115.5 | 5427g | 10 | `21:56 (1315.7s)` | Protoplasm Harness (`9:59 (599.2s)`) | Bandlepipes (`17:26 (1045.8s)`) |
| `jungle` | **Jayce** | 1 | IG Wei | 83 | 4341g | 8 | 128 | 6501g | 10 | `—` | Voltaic Cyclosword (`10:53 (652.9s)`) | Bastionbreaker (`16:53 (1012.9s)`) |
| `jungle` | **Maokai** | 1 | TES Tian | 78 | 3796g | 7 | 111 | 5455g | 9 | `30:52 (1851.9s)` | Liandry's Torment (`9:56 (595.5s)`) | Bloodletter's Curse (`20:52 (1251.9s)`) |
| `jungle` | **MonkeyKing** | 1 | IG Wei | 65 | 3214g | 7 | 112 | 5711g | 10 | `41:51 (2511.3s)` | Trinity Force (`12:51 (771.4s)`) | Black Cleaver (`19:52 (1191.5s)`) |
| `jungle` | **Naafiri** | 1 | IG Wei | 91 | 4566g | 8 | 129 | 6653g | 11 | `—` | Voltaic Cyclosword (`8:58 (537.8s)`) | Black Cleaver (`15:52 (951.9s)`) |
| `jungle` | **Vi** | 1 | TES Tian | 84 | 3956g | 8 | 134 | 6051g | 11 | `—` | Trinity Force (`11:23 (683.3s)`) | Black Cleaver (`20:04 (1204.4s)`) |
| `jungle` | **XinZhao** | 1 | TES Tian | 63 | 3094g | 7 | 109 | 5015g | 10 | `32:52 (1971.8s)` | Titanic Hydra (`14:52 (891.5s)`) | Black Cleaver (`19:53 (1193.4s)`) |
| `mid` | **Cassiopeia** | 3 | G2 Caps, HLE Zeka, TES Creme | 96.7 | 3883g | 8 | 147.3 | 5867g | 10.7 | `10:42 (642.5s)` | Rod of Ages (`12:40 (760.0s)`) | Rylai's Crystal Scepter (`19:22 (1162.3s)`) |
| `mid` | **Syndra** | 3 | HLE Zeka, IG Rookie | 96 | 3661g | 8.3 | 148.7 | 5880g | 11 | `5:55 (355.3s)` | Blackfire Torch (`12:55 (774.6s)`) | Hextech Rocketbelt (`19:58 (1198.0s)`) |
| `mid` | **Orianna** | 2 | BLG Knight, TES Creme | 97.5 | 3646g | 9 | 153 | 5828g | 11 | `9:53 (593.4s)` | Luden's Echo (`12:23 (743.2s)`) | Hextech Rocketbelt (`18:53 (1133.2s)`) |
| `mid` | **Annie** | 1 | IG Rookie | 95 | 4060g | 9 | 144 | 5998g | 11 | `3:53 (232.9s)` | Malignance (`11:53 (712.9s)`) | Hextech Rocketbelt (`19:53 (1192.9s)`) |
| `mid` | **Aurora** | 1 | TES Creme | 92 | 3556g | 9 | 149 | 5989g | 11 | `9:52 (591.9s)` | Hextech Rocketbelt (`13:52 (831.9s)`) | Shadowflame (`19:56 (1195.6s)`) |
| `mid` | **Galio** | 1 | G2 Caps | 93 | 3374g | 9 | 153 | 5370g | 11 | `13:47 (826.7s)` | Hollow Radiance (`13:47 (826.7s)`) | Imperial Mandate (`21:46 (1306.5s)`) |
| `mid` | **Locke** | 1 | BLG Knight | 95 | 3808g | 9 | 144 | 5414g | 11 | `6:59 (418.9s)` | Stormsurge (`14:49 (888.9s)`) | Lich Bane (`20:04 (1204.5s)`) |
| `mid` | **Swain** | 1 | HLE Zeka | 103 | 3654g | 9 | 151 | 5652g | 11 | `12:57 (776.6s)` | Rylai's Crystal Scepter (`10:04 (603.5s)`) | Liandry's Torment (`21:46 (1306.5s)`) |
| `mid` | **Taliyah** | 1 | IG Rookie | 103 | 3743g | 9 | 154 | 5634g | 11 | `8:52 (531.9s)` | Blackfire Torch (`14:52 (891.9s)`) | Rylai's Crystal Scepter (`18:52 (1131.9s)`) |
| `mid` | **Viktor** | 1 | IG Rookie | 87 | 3706g | 8 | 144 | 5394g | 11 | `5:51 (351.3s)` | Blackfire Torch (`13:52 (831.9s)`) | Hextech Rocketbelt (`19:52 (1191.5s)`) |
| `mid` | **Yone** | 1 | TES Creme | 71 | 2983g | 8 | 121 | 4825g | 10 | `8:53 (532.9s)` | Blade of The Ruined King (`16:53 (1012.9s)`) | Immortal Shieldbow (`22:53 (1372.9s)`) |
| `bottom` | **Corki** | 2 | HLE Gumayusi | 93.5 | 3651g | 7 | 149.5 | 6034g | 10 | `20:26 (1226.4s)` | Trinity Force (`13:51 (831.3s)`) | Manamune (`18:26 (1106.2s)`) |
| `bottom` | **Ezreal** | 2 | BLG Viper, TES JackeyLove | 88 | 3538g | 7 | 140 | 5861g | 9.5 | `20:30 (1230.2s)` | Trinity Force (`13:08 (788.5s)`) | Manamune (`22:09 (1329.2s)`) |
| `bottom` | **Jhin** | 2 | G2 Hans Sama, IG JiaQi | 90 | 3544g | 7.5 | 135.5 | 5772g | 9.5 | `7:48 (467.6s)` | Hubris (`12:55 (774.7s)`) | Phantom Dancer (`18:55 (1134.9s)`) |
| `bottom` | **Lucian** | 2 | HLE Gumayusi, TES JackeyLove | 93 | 4011g | 7 | 148.5 | 6610g | 9.5 | `15:23 (922.8s)` | Essence Reaver (`9:57 (596.6s)`) | Navori Flickerblade (`16:54 (1014.2s)`) |
| `bottom` | **Yunara** | 2 | BLG Viper, IG JiaQi | 89 | 3884g | 7 | 150 | 6315g | 9.5 | `29:55 (1795.3s)` | Kraken Slayer (`10:55 (655.4s)`) | Runaan's Hurricane (`16:25 (985.3s)`) |
| `bottom` | **Ashe** | 1 | TES JackeyLove | 88 | 3504g | 7 | 131 | 5327g | 8 | `13:53 (832.9s)` | Hexoptics C44 (`9:53 (592.9s)`) | Phantom Dancer (`18:53 (1132.9s)`) |
| `bottom` | **Caitlyn** | 1 | HLE Gumayusi | 100 | 3742g | 7 | 162 | 5896g | 9 | `5:00 (299.7s)` | Hexoptics C44 (`12:59 (778.6s)`) | Infinity Edge (`20:59 (1258.8s)`) |
| `bottom` | **Kaisa** | 1 | IG JiaQi | 92 | 3459g | 7 | 134 | 5696g | 9 | `18:04 (1084.5s)` | Kraken Slayer (`13:04 (784.4s)`) | Guinsoo's Rageblade (`16:20 (979.8s)`) |
| `bottom` | **Kalista** | 1 | TES JackeyLove | 83 | 3926g | 7 | 140 | 6135g | 9 | `12:51 (771.4s)` | Statikk Shiv (`10:52 (651.9s)`) | — |
| `bottom` | **Xayah** | 1 | G2 Hans Sama | 70 | 3108g | 6 | 119 | 5692g | 8 | `10:57 (656.7s)` | Essence Reaver (`14:47 (886.9s)`) | Infinity Edge (`21:46 (1306.5s)`) |
| `bottom` | **Zeri** | 1 | IG JiaQi | 91 | 3880g | 7 | 148 | 6469g | 9 | `—` | Stormrazor (`10:53 (652.9s)`) | Runaan's Hurricane (`15:59 (959.4s)`) |
| `support` | **Alistar** | 3 | HLE Delight, IG Meiko | 16.7 | 2210g | 5.3 | 25.3 | 3512g | 7 | `18:55 (1134.8s)` | Zeke's Convergence (`16:35 (994.8s)`) | Knight's Vow (`29:54 (1793.6s)`) |
| `support` | **Nautilus** | 3 | BLG ON, G2 Labrov, IG Meiko | 14.3 | 2378g | 5.3 | 21.7 | 3573g | 6.3 | `18:30 (1109.7s)` | Locket of the Iron Solari (`13:19 (799.1s)`) | Knight's Vow (`28:56 (1735.7s)`) |
| `support` | **Lulu** | 2 | BLG ON, IG Meiko | 15 | 2410g | 6 | 20.5 | 3604g | 8 | `14:52 (891.5s)` | Ardent Censer (`12:57 (776.9s)`) | Mikael's Blessing (`24:25 (1465.2s)`) |
| `support` | **Milio** | 2 | HLE Delight, TES Zhuo | 18 | 2662g | 6 | 24 | 3882g | 7.5 | `19:26 (1166.2s)` | Echoes of Helia (`12:54 (774.2s)`) | Mikael's Blessing (`25:54 (1554.3s)`) |
| `support` | **Karma** | 1 | HLE Delight | 22 | 2381g | 6 | 31 | 3457g | 8 | `29:48 (1788.5s)` | Shurelya's Battlesong (`13:54 (834.0s)`) | Mikael's Blessing (`26:59 (1618.8s)`) |
| `support` | **Rakan** | 1 | G2 Labrov | 16 | 2637g | 6 | 20 | 3823g | 7 | `—` | Knight's Vow (`13:06 (785.7s)`) | Locket of the Iron Solari (`22:57 (1376.7s)`) |
| `support` | **Rell** | 1 | TES Zhuo | 17 | 2413g | 6 | 23 | 3650g | 6 | `21:15 (1274.9s)` | Zeke's Convergence (`15:15 (914.6s)`) | — |
| `support` | **Renata** | 1 | TES Zhuo | 12 | 2512g | 6 | 19 | 3837g | 8 | `17:00 (1020.4s)` | Knight's Vow (`12:51 (771.4s)`) | Mikael's Blessing (`23:51 (1431.3s)`) |
| `support` | **Seraphine** | 1 | TES Zhuo | 16 | 2877g | 6 | 21 | 3936g | 8 | `18:53 (1132.9s)` | Mikael's Blessing (`16:53 (1012.9s)`) | Moonstone Renewer (`28:53 (1732.9s)`) |
| `support` | **Yuumi** | 1 | IG Meiko | 12 | 2504g | 6 | 15 | 3872g | 8 | `—` | Ardent Censer (`11:55 (715.4s)`) | Mikael's Blessing (`22:53 (1372.9s)`) |

---

## 4. Reproducibility

- **Standalone CLI Script**: [`prototype/pro-benchmark/ingest_and_align_prototype.py`](./ingest_and_align_prototype.py)
  ```bash
  python3 prototype/pro-benchmark/ingest_and_align_prototype.py --delay 0.15 --output prototype/pro-benchmark/pro_benchmark_sample_baselines.json
  ```
- **Spring Boot Opt-In CLI Runner**: `ProBenchmarkPrototypeRunner` (runs only when `--run-pro-benchmark-prototype` is passed; completely inert during normal backend startup).
- **Private Baseline Dataset**: [`prototype/pro-benchmark/pro_benchmark_sample_baselines.json`](./pro_benchmark_sample_baselines.json)
