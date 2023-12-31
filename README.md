# Model Summary

## factor structure

| Factor  | Input                   | data source                                | Fun                                  | Parameters               | Derived                                                                          |
|---------|-------------------------|--------------------------------------------|--------------------------------------|--------------------------|----------------------------------------------------------------------------------|
| MTM     | R                       | major_return.db                            | .                                    | None                     | Sum[T]X, Sum[T]X/Std[T]X, T=(10,21,63,126,189,252)                               |
| SIZE    | OI,contractMultiplier,P | instrument_volume.db                       | .                                    | None                     | Aver[T]X, X / Aver[T]X - 1, T=(10,21,63,126,189,252), X / X[L] - 1 L=(21,63,252) |
| OI      | OI                      | instrument_volume.db                       | .                                    | None                     | X / Aver[T]X - 1, T=(10,21,63,126,189,252), X / X[L] - 1 L=(21,63,252)           |
| RS      | RS                      | fund_stock_by_instru.csv                   | .                                    | None                     | X / Aver[T]X - 1, T=(10,21,63,126,189,252), X / X[L] - 1 L=(21,63,252)           |
| BASIS   | basisRate               | fund_basis_by_instru.csv                   | .                                    | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252)         |
| TS      | P,Pmin                  | major_minor.db, md_by_instru.csv           | (P/Pmin -1)*12/MonthD                | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252)         |
| LIQUID  | amt,R                   | major_return.db                            | abs(R)/amt                           | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252)         |
| SR      | VOL,OI                  | instrument_volume.db                       | VOL/OI                               | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252)         |
| HR      | VOL,OI                  | instrument_volume.db                       | dOI/VOL                              | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252)         |
| NETOI   | OI,L,S                  | instrument_volume.db, instrument_member.db | (sum(L)-sum(S))/OI                   | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252)         |
| NETOIW  | OI,L,S                  | instrument_volume.db, instrument_member.db | (w_sum(L)-w_sum(S))/OI               | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252)         |
| NETDOI  | OI,L,S                  | instrument_volume.db, instrument_member.db | (sum(dL)-sum(dS))/OI                 | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252)         |
| NETDOIW | OI,L,S                  | instrument_volume.db, instrument_member.db | (w_sum(dL)-w_sum(dS))/OI             | None                     | Aver[T]X, X - Aver[T]X, T=(10,21,63,126,189,252), X - X[L] L=(21,63,252)         |
| SKEW    | R                       | major_return.db                            | Skew[T]R                             | T=(10,21,63,126,189,252) | X - X[L] L=(21,63,252)                                                           |
| VOL     | R                       | major_return.db                            | Std[T]R                              | T=(10,21,63,126,189,252) | X - X[L] L=(21,63,252)                                                           |
| RVOL    | open,high,low,close     | major_return.db                            | {ln(h/o)ln(h/c) + ln(l/o)ln(l/c)}[T] | T=(10,21,63,126,189,252) | X - X[L] L=(21,63,252)                                                           |
| CV      | R                       | major_return.db                            | Std[T]R/abs(Aver[T]R)                | T=(10,21,63,126,189,252) | X - X[L] L=(21,63,252)                                                           |
| CTP     | vol,oi,P                | major_return.db                            | Corr[T](vol/oi, P)                   | T=(63,126,189,252)       | X - X[L] L=(21,63,252)                                                           |
| CVP     | vol,P                   | major_return.db                            | Corr[T](vol, P)                      | T=(63,126,189,252)       | X - X[L] L=(21,63,252)                                                           |
| CSP     | R,P                     | major_return.db                            | Corr[T](Std[21]R, P)                 | T=(63,126,189,252)       | X - X[L] L=(21,63,252)                                                           |
| BETA    | R,RM                    | major_return.db, market_return             | COV[T]{R,RM}/VAR[T]{RM}              | T=(21,63,126,189,252)    | X - X[L] L=(21,63,252)                                                           |
| VAL     | P                       | major_return.db                            | AVER[21]P/Aver[T]P                   | T=(126,252,378,504)      | X - X[L] L=(21,63,252)                                                           |
| CBETA   | R,RC                    | major_return.db, forex exchange rate       | COV[T]{R,RC}/VAR[T]{RC}              | T=(126,252,378,504)      | X - X[L] L=(21,63,252)                                                           |
| IBETA   | R,RC                    | major_return.db, macro economic            | COV[T]{R,RC}/VAR[T]{RC}              | T=(126,252,378,504)      | X - X[L] L=(21,63,252)                                                           |
| MACD    | (O,H,L,C)               | major_return.db                            | MACD(F, S, ALPHA)                    | (F=10, S=21, ALPHA=0.2)  |                                                                                  |
| KDJ     | (O,H,L,C)               | major_return.db                            | KDJ(N)                               | (N=10, 15)               |                                                                                  |
| RSI     | (O,H,L,C)               | major_return.db                            | RSI(N)                               | (N=10, 15)               |                                                                                  |

## data involved

+ major: return, amt, vol, oi, P(close)
+ minor: P(close)
+ agg_by_instrument: P(close), VOL, OI, AMT
+ others: basisRate, registerStock
+ instru_idx: open,high,low,close

## Class factors

+ CReaderMarketReturn
+ CReaderExchangeRate
+ CReaderMacroEconomic
+ CDbByInstrument
+ CCSVByInstrument
    + CMdByInstrument
    + CFundByInstrument

+ CFactors: Core(_set_factor_id(), _get_update_df(_get_instrument_factor_exposure())), _truncate_series(), _truncate_dataFrame()
    + CFactorsWithMajorReturn
        + CFactorMTM
        + CFactorsWithMajorReturnAndArgWin: _set_base_date()
            + CFactorsSKEW: _set_factor_id(), _get_instrument_factor_exposure()
            + CFactorsVOL: _set_factor_id(), _get_instrument_factor_exposure()
            + CFactorsRVOL: _set_factor_id(), _get_instrument_factor_exposure()
            + CFactorsCV: _set_factor_id(), _get_instrument_factor_exposure()
            + CFactorsCTP: _set_factor_id(), _get_instrument_factor_exposure()
            + CFactorsCVP: _set_factor_id(), _get_instrument_factor_exposure()
            + CFactorsCSP: _set_factor_id(), _get_instrument_factor_exposure(), _set_base_date()
            + CFactorsVAL: _set_factor_id(), _get_instrument_factor_exposure()
            + CFactorsWithMajorReturnAndMarketReturn
                + CFactorsBETA: _set_factor_id(), _get_instrument_factor_exposure()
            + CFactorsWithMajorReturnAndExchangeRate
                + CFactorsCBETA: _set_factor_id(), _get_instrument_factor_exposure()
            + CFactorsWithMajorReturnAndMacroEconomic
                + CFactorsIBETA: _set_factor_id(), _get_instrument_factor_exposure()
    + CFactorsWithBasis
    + CFactorsWithStock
    + CFactorsWithMajorMinorAndMdc
    + CFactorsWithInstruVolume
    + CFactorsWithInstruVolumeAndInstruMember

+ CFactors: Core(_set_factor_id(), _get_update_df(_get_instrument_factor_exposure())), _truncate_series(), _truncate_dataFrame()
    + CFactorsTransformer:  _get_update_df(_set_base_date(), _transform())
    + CFactorsTransformerSum: _set_factor_id(), _transform()
    + CFactorsTransformerAver: _set_factor_id(), _transform()
    + CFactorsTransformerSharpe: _set_factor_id(), _transform()
    + CFactorsTransformerBreakRatio: _set_factor_id(), _transform()
    + CFactorsTransformerBreakDiff: _set_factor_id(), _transform()
    + CFactorsTransformerLagRatio: _set_factor_id(), _transform(), _set_base_date()
    + CFactorsTransformerLagDiff: _set_factor_id(), _transform(), _set_base_date()