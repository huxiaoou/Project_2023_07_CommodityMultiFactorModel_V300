# Model Summary

## factor structure

| Factor     | Input                   | data source                                  | Fun                                  | Parameters               | Derived                                                                  | Moving Average |
|------------|-------------------------|----------------------------------------------|--------------------------------------|--------------------------|--------------------------------------------------------------------------|----------------|
| MTM        | R                       | * major_return.db                            | .                                    | None                     | Sum[T]X, Sum[T]X/Std[T]X, T=(10,21,63,126,189,252)                       | (5,10,15)      |
| BASIS      | basisRate               | * fund_basis_by_instru.csv                   | .                                    | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252) | (5,10,15)      |
| TS         | P,Pmin                  | * major_minor.db, md_by_instru.csv           | (P/Pmin -1)*12/MonthD                | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252) | (5,10,15)      |
| LIQUIDITY  | amt,R                   | * major_return.db                            | abs(R)/amt                           | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252) | (5,10,15)      |
| SR         | VOL,OI                  | * instrument_volume.db                       | VOL/OI                               | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252) | (5,10,15)      |
| HR         | VOL,OI                  | * instrument_volume.db                       | dOI/VOL                              | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252) | (5,10,15)      |
| NETOI      | OI,L,S                  | * instrument_volume.db, instrument_member.db | (sum(L)-sum(S))/OI                   | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252) | (5,10,15)      |
| NETOIW     | OI,L,S                  | * instrument_volume.db, instrument_member.db | (w_sum(L)-w_sum(S))/OI               | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252) | (5,10,15)      |
| NETDOI     | OI,L,S                  | * instrument_volume.db, instrument_member.db | (sum(dL)-w_sum(dS))/OI               | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252) | (5,10,15)      |
| SKEW       | R                       | * major_return.db                            | Skew[T]R                             | T=(10,21,63,126,189,252) | X - X[L] L=(21,63,252)                                                   | (5,10,15)      |
| VOLATILITY | R                       | * major_return.db                            | Std[T]R                              | T=(10,21,63,126,189,252) | X - X[L] L=(21,63,252)                                                   | (5,10,15)      |
| CV         | R                       | * major_return.db                            | Std[T]R/abs(Aver[T]R)                | T=(10,21,63,126,189,252) | X - X[L] L=(21,63,252)                                                   | (5,10,15)      |
| SCV        | R                       | * major_return.db                            | Std[T]R/Aver[T]R                     | T=(10,21,63,126,189,252) | X - X[L] L=(21,63,252)                                                   | (5,10,15)      |
| RSVOL      | open,high,low,close     | * major_return.db                            | {ln(h/o)ln(h/c) + ln(l/o)ln(l/c)}[T] | T=(10,21,63,126,189,252) | X - X[L] L=(21,63,252)                                                   | (5,10,15)      |
| OI         | OI                      | * instrument_volume.db                       | OI/Aver[T]OI-1                       | T=(10,21,63,126,189,252) | X - X[L] L=(21,63,252)                                                   | (5,10,15)      |
| SIZE       | OI,contractMultiplier,P | * instrument_volume.db                       | P*OI*contractMultiplier              | T=(10,21,63,126,189,252) | X - X[L] L=(21,63,252)                                                   | (5,10,15)      |
| RSW        | RS                      | * fund_stock_by_instru.csv                   | RS/W-AVER[T]RS-1                     | T=(63,126,189,252)       | X - X[L] L=(21,63,252)                                                   | (5,10,15)      |
| CTP        | vol,oi,P                | * major_return.db                            | Corr[T](vol/oi, P)                   | T=(63,126,189,252)       | X - X[L] L=(21,63,252)                                                   | (5,10,15)      |
| CVP        | vol,P                   | * major_return.db                            | Corr[T](vol, P)                      | T=(63,126,189,252)       | X - X[L] L=(21,63,252)                                                   | (5,10,15)      |
| CSP        | R,P                     | * major_return.db                            | Corr[T](Std[21]R, P)                 | T=(63,126,189,252)       | X - X[L] L=(21,63,252)                                                   | (5,10,15)      |
| VALUE      | P                       | * major_return.db                            | AVER[21]P/Aver[T]P                   | T=(63,126,252,504)       | X - X[L] L=(21,63,252)                                                   | (5,10,15)      |
| BETA       | R,RM                    | * major_return.db, market_return             | COV[T]{R,RM}/VAR[T]{RM}              | T=(63,126,252,504)       | X - X[L] L=(21,63,252)                                                   | (5,10,15)      |
| CBETA      | R,RC                    | * major_return.db, forex exchange rate       | COV[T]{R,RC}/VAR[T]{RC}              | T=(63,126,252,504)       | X - X[L] L=(21,63,252)                                                   | (5,10,15)      |
| IBETA      | R,RC                    | * major_return.db, macro economic            | COV[T]{R,RC}/VAR[T]{RC}              | T=(63,126,252,504)       | X - X[L] L=(21,63,252)                                                   | (5,10,15)      |
| MACD       | (O,H,L,C)               | * major_return.db                            | MACD(F, S, ALPHA)                    | (F=10, S=21, ALPHA=0.2)  |                                                                          | (5,10,15)      |
| KDJ        | (O,H,L,C)               | * major_return.db                            | KDJ(N)                               | (N=10, 15)               |                                                                          | (5,10,15)      |
| RSI        | (O,H,L,C)               | * major_return.db                            | RSI(N)                               | (N=10, 15)               |                                                                          | (5,10,15)      |

## data involved

+ major: return, amt, vol, oi, P(close)
+ minor: P(close)
+ agg_by_instrument: P(close), VOL, OI, AMT
+ others: basisRate, registerStock
+ instru_idx: open,high,low,close
