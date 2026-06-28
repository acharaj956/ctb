package org.openidc;

/**
 * Event screening — the heart of CTBT verification: deciding whether a detected
 * event looks like a natural earthquake or a possible explosion.
 *
 * Real IDC screening applies several criteria, chiefly:
 *   - event DEPTH: events deeper than a few tens of km are certainly natural,
 *     because explosions are shallow (here: depth > minNaturalDepthKm -> "eq");
 *   - the mb:Ms discriminant: explosions are richer in high-frequency (body-wave)
 *     energy, so they show a large body-wave magnitude (mb) relative to their
 *     surface-wave magnitude (Ms).
 *
 * This implementation applies the depth criterion and a simplified shallow-and-
 * high-mb rule as a stand-in for the full mb:Ms measurement (Ms is not computed
 * by the simplified locator). The intent is to demonstrate the screening
 * decision step, not to be a physically rigorous discriminant.
 *
 * Returns a CSS `etype`: "eq" (earthquake-like) or "ex" (explosion-like).
 */
public class Screener {

    private final double maxNaturalShallowMb;  // shallow events above this screen as explosion-like
    private final double minNaturalDepthKm;    // events deeper than this are certainly natural

    public Screener(double maxNaturalShallowMb, double minNaturalDepthKm) {
        this.maxNaturalShallowMb = maxNaturalShallowMb;
        this.minNaturalDepthKm = minNaturalDepthKm;
    }

    public String screen(Locator.Origin o) {
        if (o.depth > minNaturalDepthKm) {
            return "eq";  // deep -> certainly a natural earthquake
        }
        return o.mb >= maxNaturalShallowMb ? "ex" : "eq";
    }
}
