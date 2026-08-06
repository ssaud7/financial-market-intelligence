# Data contract scorecard

Generated: 2026-08-05T23:50:51.085618+00:00

- Messages produced: **25000**
- Messages consumed: **25000**
- Admitted to landing zone: **22996**
- Routed to quarantine + dead-letter topic: **2004**
- Injected faults escaping the contract: **0**
- Contract catch rate: **1.0**

| Injected fault | Injected | Quarantined | Escaped | Catch rate |
| --- | ---: | ---: | ---: | ---: |
| `blank_ticker` | 170 | 170 | 0 | 1.0 |
| `corrupt_timestamp` | 168 | 168 | 0 | 1.0 |
| `future_date` | 167 | 167 | 0 | 1.0 |
| `high_below_low` | 184 | 184 | 0 | 1.0 |
| `missing_close` | 166 | 166 | 0 | 1.0 |
| `missing_ticker` | 141 | 141 | 0 | 1.0 |
| `negative_price` | 164 | 164 | 0 | 1.0 |
| `negative_volume` | 184 | 184 | 0 | 1.0 |
| `non_numeric_price` | 169 | 169 | 0 | 1.0 |
| `truncated_json` | 168 | 168 | 0 | 1.0 |
| `unexpected_field` | 166 | 166 | 0 | 1.0 |
| `zero_price` | 157 | 157 | 0 | 1.0 |

## Rejections by reason code

| Reason | Count |
| --- | ---: |
| `FUTURE_DATED` | 167 |
| `INVALID_TICKER` | 170 |
| `INVALID_TIMESTAMP` | 168 |
| `MALFORMED_JSON` | 168 |
| `MISSING_FIELD` | 307 |
| `NEGATIVE_VOLUME` | 184 |
| `NON_POSITIVE_PRICE` | 321 |
| `OHLC_INCONSISTENT` | 184 |
| `WRONG_TYPE` | 335 |
