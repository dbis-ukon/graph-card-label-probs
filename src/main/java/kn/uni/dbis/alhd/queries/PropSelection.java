package kn.uni.dbis.alhd.queries;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

public class PropSelection {

    public final String property;
    public final double[] range;
    public final String value;

    public static PropSelection merge(final PropSelection a, final PropSelection b) {
        if (!Objects.equals(a.property, b.property)) {
            throw new IllegalArgumentException("Properties must match.");
        }

        if (a.range == null ^ b.range == null) {
            throw new IllegalArgumentException("Type of property must match.");
        }
        if (a.range == null) {
            if (!Objects.equals(a.value, b.value)) {
                throw new IllegalArgumentException("Values don't match: " + a + " vs. " + b);
            }
            return a;
        }
        return new PropSelection(a.property, Math.max(a.range[0], b.range[0]), Math.min(a.range[1], b.range[1]));
    }

    public PropSelection(String property, double low, double high) {
        if (low > high) {
            throw new IllegalArgumentException(
                    String.format(Locale.US, "Broken range: %s = [%f, %f])", property, low, high));
        }
        this.property = property;
        this.range = new double[] { low, high };
        this.value = null;
    }

    public PropSelection(final String prop, final String op, final double value) {
        this.property = prop;
        double[] range = null;
        switch (op) {
            case "<":
                range = new double[] { Double.NEGATIVE_INFINITY, value };
                break;
            case ">":
                range = new double[] { value, Double.POSITIVE_INFINITY };
                break;
            case "=":
                range = new double[] { value, value };
                break;
            default:
                throw new AssertionError("Unknown comparison operator: " + op);
        }
        this.range = range;
        this.value = null;
    }

    public PropSelection(final String prop, final String value) {
        this.property = prop;
        this.range = null;
        this.value = value;
    }

    @Override
    public String toString() {
        return "PropSelection[" + property + (range == null ? "='" + this.value + "'"
                : range[0] == range[1] ? "=" + range[0]
                : Double.isInfinite(range[0]) ? "<" + range[1]
                : Double.isInfinite(range[1]) ? ">" + range[0]
                : " in [" + range[0] + ", " + range[1]) + ']';
    }

    public void toString(final StringBuilder sb, final String var) {
        if (this.value != null) {
            final String str = this.value.replace("\\", "\\\\").replace("'", "\\'");
            sb.append(String.format(Locale.US, "%s.%s = '%s'", var, property, str));
        } else if (Double.isInfinite(range[0])) {
            sb.append(String.format(Locale.US, "%s.%s < %f", var, property, range[1]));
        } else if (Double.isInfinite(range[1])) {
            sb.append(String.format(Locale.US, "%s.%s > %f", var, property, range[0]));
        } else if (range[0] == range[1]) {
            sb.append(String.format(Locale.US, "%s.%s = %f", var, property, range[1]));
        } else {
            sb.append(String.format(Locale.US, "%f < %s.%s < %f", range[0], var, property, range[1]));
        }
    }

    public Optional<String> valueString() {
        if (this.value != null) {
            final String str = this.value.replace("\\", "\\\\").replace("\"", "\\\"");
            return Optional.of("\"" + str + "\"");
        }
        if (this.range[0] != this.range[1]) {
            return Optional.empty();
        }
        final double val = this.range[0];
        if (((int) val) == val) {
            return Optional.of(String.format("\"%d\"^^<http://www.w3.org/2001/XMLSchema#int>", (int) val));
        }
        if (((long) val) == val) {
            return Optional.of(String.format("\"%d\"^^<http://www.w3.org/2001/XMLSchema#long>", (long) val));
        }
        return Optional.of(String.format("\"%s\"^^<http://www.w3.org/2001/XMLSchema#double>", val));
    }
}
