# Model Summary

## factor structure

| Factor     | Input                   | Fun                                  | Parameters              | Derived                                                               | Moving Average |
|------------|-------------------------|--------------------------------------|-------------------------|-----------------------------------------------------------------------|----------------|
| MTM        | R                       | .                                    | None                    | Sum[T]X, Sum[T]X/Std[T]X, T=(21,63,126,189,252)                       | (5,10,15)      |
| BASIS      | basisRate               | .                                    | None                    | Aver[T]X, X - Aver[T]X, T=(21,63,126,189,252), X - X[L] L=(21,63,252) | (5,10,15)      |
| TS         | P,Pmin                  | (P/Pmin -1)*12/MonthD                | None                    | Aver[T]X, X - Aver[T]X, T=(21,63,126,189,252), X - X[L] L=(21,63,252) | (5,10,15)      |
| LIQUIDITY  | AMT,R                   | abs(Rmaj)/AMT                        | None                    | Aver[T]X, X - Aver[T]X, T=(21,63,126,189,252), X - X[L] L=(21,63,252) | (5,10,15)      |
| SR         | VOL,OI                  | VOL/OI                               | None                    | Aver[T]X, X - Aver[T]X, T=(21,63,126,189,252), X - X[L] L=(21,63,252) | (5,10,15)      |
| HR         | VOL,OI                  | dOI/VOL                              | None                    | Aver[T]X, X - Aver[T]X, T=(21,63,126,189,252), X - X[L] L=(21,63,252) | (5,10,15)      |
| NETOI      | OI,L,S                  | (sum(L)-sum(S))/OI                   | None                    | Aver[T]X, X - Aver[T]X, T=(21,63,126,189,252), X - X[L] L=(21,63,252) | (5,10,15)      |
| NETOIW     | OI,L,S                  | (w_sum(L)-w_sum(S))/OI               | None                    | Aver[T]X, X - Aver[T]X, T=(21,63,126,189,252), X - X[L] L=(21,63,252) | (5,10,15)      |
| SKEW       | R                       | Skew[T]R                             | T=(10,21,63,126,252)    | X - X[L] L=(21,63,252)                                                | (5,10,15)      |
| VOLATILITY | R                       | Std[T]R                              | T=(10,21,63,126,252)    | X - X[L] L=(21,63,252)                                                | (5,10,15)      |
| CV         | R                       | Std[T]R/abs(Aver[T]R)                | T=(10,21,63,126,252)    | X - X[L] L=(21,63,252)                                                | (5,10,15)      |
| SCV        | R                       | Std[T]R/Aver[T]R                     | T=(10,21,63,126,252)    | X - X[L] L=(21,63,252)                                                | (5,10,15)      |
| RSVOL      | open,high,low,close     | {ln(h/o)ln(h/c) + ln(l/o)ln(l/c)}[T] | T=(10,21,63,126,252)    | X - X[L] L=(21,63,252)                                                | (5,10,15)      |
| OI         | OI                      | OI/Aver[T]OI-1                       | T=(10,21,63,126,252)    | X - X[L] L=(21,63,252)                                                | (5,10,15)      |
| SIZE       | OI,contractMultiplier,P | P*OI*contractMultiplier              | T=(10,21,63,126,252)    | X - X[L] L=(21,63,252)                                                | (5,10,15)      |
| RSW        | RS                      | RS/W-AVER[T]RS-1                     | T=(63,126,189,252)      | X - X[L] L=(21,63,252)                                                | (5,10,15)      |
| CTP        | VOL,OI,P                | Corr[T](VOL/OI, P)                   | T=(63,126,189,252)      | X - X[L] L=(21,63,252)                                                | (5,10,15)      |
| CVP        | VOL,P                   | Corr[T](VOL, P)                      | T=(63,126,189,252)      | X - X[L] L=(21,63,252)                                                | (5,10,15)      |
| CSP        | VOL,P                   | Corr[T](Std[21], P)                  | T=(63,126,189,252)      | X - X[L] L=(21,63,252)                                                | (5,10,15)      |
| VALUE      | P                       | AVER[21]P/Aver[T]P                   | T=(63,126,252,504)      | X - X[L] L=(21,63,252)                                                | (5,10,15)      |
| BETA       | R,RM                    | COV[T]{R,RM}/VAR[T]{RM}              | T=(63,126,252,504)      | X - X[L] L=(21,63,252)                                                | (5,10,15)      |
| CBETA      | R,RC                    | COV[T]{R,RC}/VAR[T]{RC}              | T=(63,126,252,504)      | X - X[L] L=(21,63,252)                                                | (5,10,15)      |
| MACD       | (O,H,L,C)               | MACD(F, S, ALPHA)                    | (F=10, S=21, ALPHA=0.2) |                                                                       | (5,10,15)      |
| KDJ        | (O,H,L,C)               | KDJ(N)                               | (N=10, 15)              |                                                                       | (5,10,15)      |
| RSI        | (O,H,L,C)               | RSI(N)                               | (N=10, 15)              |                                                                       | (5,10,15)      |

## data involved

+ major: return, amt, vol, oi, P(close)
+ minor: P(close)
+ agg_by_instrument: P(close), VOL, OI, AMT
+ others: basisRate, registerStock
+ instru_idx: open,high,low,close
