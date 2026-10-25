# POD bot settled record

Source: exported bet log, bets with start times from 2026-08-10 to 2026-10-12. 1,426 rows, 248 unsettled and excluded. Amounts in dollars, stakes of $20 or $25. Expected profit = stake x logged EV, where EV = bet odds / devigged Pinnacle price - 1.

### All settled bets

| Bets | Staked | Profit | Yield | Expected yield | Std. error of yield | Win rate |
|---:|---:|---:|---:|---:|---:|---:|
| 1,178 | $29,430 | $619.13 | +2.10% | +4.90% | ±3.25% | 43.7% |

### By logged EV

| | Bets | Staked | Profit | Yield | Expected yield | Std. error of yield |
|---|---:|---:|---:|---:|---:|---:|
| EV below 0% | 96 | $2,400 | $-240.50 | -10.02% | -7.65% | ±11.41% |
| 0% to 3% | 338 | $8,445 | $-628.62 | -7.44% | +2.06% | ±5.83% |
| 3% to 5% | 290 | $7,245 | $518.75 | +7.16% | +3.90% | ±6.38% |
| 5% to 10% | 288 | $7,190 | $348.75 | +4.85% | +6.87% | ±6.85% |
| 10% and above | 166 | $4,150 | $620.75 | +14.96% | +16.28% | ±9.07% |


### By sport

| | Bets | Staked | Profit | Yield | Expected yield | Std. error of yield |
|---|---:|---:|---:|---:|---:|---:|
| American Football | 3 | $75 | $46.50 | +62.00% | +14.71% | ±81.59% |
| Baseball | 13 | $325 | $37.75 | +11.62% | +2.75% | ±27.12% |
| Basketball | 73 | $1,825 | $70.75 | +3.88% | +4.47% | ±11.20% |
| Handball | 83 | $2,075 | $-26.50 | -1.28% | +14.02% | ±12.37% |
| MMA | 3 | $75 | $-19.25 | -25.67% | +16.14% | ±74.33% |
| Soccer | 825 | $20,620 | $-167.62 | -0.81% | +3.99% | ±3.97% |
| Tennis | 88 | $2,185 | $147.75 | +6.76% | +5.20% | ±11.27% |
| Volleyball | 90 | $2,250 | $529.75 | +23.54% | +4.53% | ±11.28% |


### By market

| | Bets | Staked | Profit | Yield | Expected yield | Std. error of yield |
|---|---:|---:|---:|---:|---:|---:|
| MONEY_LINE_2_WAY | 297 | $7,410 | $879.25 | +11.87% | +7.45% | ±6.22% |
| MONEY_LINE_3_WAY | 467 | $11,670 | $115.00 | +0.99% | +3.99% | ±5.92% |
| SPREAD | 414 | $10,350 | $-375.12 | -3.62% | +4.10% | ±4.60% |


### Only bets with positive logged EV

1,082 bets, $27,030 staked, profit $859.63, yield +3.18% against +6.02% expected (std. error ±3.40%).

### Sharp price move between opening and the logged bet

Median change in the devigged price from opening to the bet: -13.21%. Share of bets where the price had shortened (our side became more likely): 85.4%.


## Reading these numbers

- Bets with a logged EV of 3% or more (744 bets) earned $1,488.25 on $18,585 staked, a +8.01% yield against +7.81% expected. Bets below 3% (434 bets) lost $869.12 on $10,845 staked. This matches the 3% cutoff used in the greyhound bot, but the sub-3% group is not large enough to prove the cutoff is optimal.
- Expected profit uses the devigged Pinnacle price at the time of the bet as the fair price. It is not a measurement against the closing line. The file holds the opening and the logged prices only, so closing line value cannot be computed from it.
- 96 settled bets have a negative logged EV. Several of them have a bet price far from the logged Pinnacle price, which points to a matching error between the soft-bookmaker outcome and the Pinnacle outcome. They are kept in the totals, not filtered out.
- The standard errors are rough and assume bets are independent. Several bets can share one event, and 10 settled rows repeat an event, market, outcome and period.
- The sample covers about two months, so the overall result cannot be separated from zero or from the expected yield.
