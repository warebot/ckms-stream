package com.warebot.ckms.stream;

/**
 * <p>
 * Quantile is an invariant for estimation with {@link CKMSStream}.
 * </p>
 */


public class Quantile {
    final double quantile;
    final double error;


    /**
     * <p>
     * Create an invariant for a quantile
     * </p>
     *
     * @param quantile The target quantile value expressed along the interval
     *                 <code>[0, 1]</code>.
     * @param error    The target error allowance expressed along the interval
     *                 <code>[0, 1]</code>.
     */
    public Quantile(final double quantile, final double error) {
        this.quantile = quantile;
        this.error = error;
    }


    @Override
    public String toString() {
        return String.format("Q{q=%f, eps=%f})", quantile, error);
    }

    public double getQuantile() {
        return quantile;
    }

    public double getError() {
        return error;
    }

}




