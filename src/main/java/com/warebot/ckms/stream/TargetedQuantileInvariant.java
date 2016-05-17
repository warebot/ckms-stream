package com.warebot.ckms.stream;

/**
 *  The invariant function responsible for calculating the delta/acceptable error for a rank.
 *
 *  Reference:
 *  Section 4; Definition 4.
 */
public class TargetedQuantileInvariant extends Invariant {
    private Quantile[] quantiles;

    public TargetedQuantileInvariant(Quantile... quantiles) {
        this.quantiles = quantiles;
    }

    public double f(double r, int n) {
        double error = n + 1;
        double f;

        for (Quantile q : quantiles) {
            if (r <= Math.floor(q.quantile * n)) {
                f = (2.0 * q.error * (n - r)) / (1.0 - q.quantile);
            } else {
                f = (2.0 * q.error * r) / q.quantile;
            }
            error = Math.min(error, f);
        }
        return error;
    }
}

