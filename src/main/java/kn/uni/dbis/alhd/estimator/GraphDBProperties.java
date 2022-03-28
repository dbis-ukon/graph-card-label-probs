package kn.uni.dbis.alhd.estimator;

import kn.uni.dbis.alhd.queries.Direction;
import kn.uni.dbis.alhd.statistics.GraphStatistics;

import java.util.OptionalDouble;

public final class GraphDBProperties extends GADbProperties {

    private final GraphStatistics stats;
    private final boolean simulateNeo4j;

    public GraphDBProperties(final LabelDistribution labelDistribution, final GraphStatistics stats,
                             final boolean simulateNeo4j) {
        super(labelDistribution);
        this.stats = stats;
        this.simulateNeo4j = simulateNeo4j;
    }

    @Override
    public double sel(int label, int property) {
        final double n = stats.numNodes(label);
        final double withProp = stats.nodeWithProperty(label, property);
        if (withProp == 0) {
            return 0;
        }
        final double withPropUnique = stats.nodeWithPropertyUnique(label, property);
        if (withPropUnique == 0) {
            throw new IllegalStateException("Cannot have " + withProp + "values but " + withPropUnique + " uniques.");
        }
        final double hasProp = withProp / n;
        final double propHasValue = 1 / withPropUnique;
        return hasProp * propHasValue;
    }

    @Override
    public double sel(int label, int property, int valueHash) {
        final double n = stats.numNodes(label);
        final double withProp = stats.nodeWithProperty(label, property);
        if (withProp == 0) {
            return 0;
        }
        final double withPropUnique = stats.nodeWithPropertyUnique(label, property);
        if (withPropUnique == 0) {
            throw new IllegalStateException("Cannot have " + withProp + "values but " + withPropUnique + " uniques.");
        }
        final double hasProp = withProp / n;
        final OptionalDouble freq = stats.nodePropIfFrequent(label, property, valueHash);
        final double propHasValue;
        if (freq.isPresent()) {
            propHasValue = freq.getAsDouble();
        } else {
            final int heavyHitters = stats.numNodePropFrequent(label, property);
            propHasValue = heavyHitters >= withPropUnique ? 0 : 1 / (withPropUnique - heavyHitters);
        }
        return hasProp * propHasValue;
    }

    @Override
    public double rangeSel(int label, int property, double min, double max) {
        final double numProps = stats.nodeWithPropertyNumeric(label, property);
        if (numProps <= 0) {
            return 0.0;
        }
        final double n = stats.numNodes(label);
        if (min == max) {
            final double withProp = stats.nodeWithProperty(label, property);
            final double withPropUnique = stats.nodeWithPropertyUnique(label, property);
            final double hasProp = withProp / n;
            final double isNumeric = numProps / withProp;
            final double propHasValue = 1 / (isNumeric * withPropUnique);
            return hasProp * isNumeric * propHasValue;
        }
        return stats.nodePropertyRange(label, property, min, max).orElse(0.0);
    }

    @Override
    public double relSel(int type, int property) {
        final double n = stats.numRelationships(type);
        final double withProp = stats.relWithProperty(type, property);
        if (withProp == 0) {
            return 0;
        }
        final double withPropUnique = stats.relWithPropertyUnique(type, property);
        if (withPropUnique == 0) {
            throw new IllegalStateException("Cannot have " + withProp + "values but " + withPropUnique + " uniques.");
        }
        final double hasProp = withProp / n;
        final double propHasValue = 1 / withPropUnique;
        return hasProp * propHasValue;
    }

    @Override
    public double relSel(int type, int property, int valueHash) {
        final double n = stats.numRelationships(type);
        final double withProp = stats.relWithProperty(type, property);
        if (withProp == 0) {
            return 0;
        }
        final double withPropUnique = stats.relWithPropertyUnique(type, property);
        if (withPropUnique == 0) {
            throw new IllegalStateException("Cannot have " + withProp + "values but " + withPropUnique + " uniques.");
        }
        final double hasProp = withProp / n;
        final OptionalDouble freq = stats.relPropIfFrequent(type, property, valueHash);
        final double propHasValue;
        if (freq.isPresent()) {
            propHasValue = freq.getAsDouble();
        } else {
            final int heavyHitters = stats.numRelPropFrequent(type, property);
            propHasValue = heavyHitters >= withPropUnique ? 0 : 1 / (withPropUnique - heavyHitters);
        }
        return hasProp * propHasValue;
    }

    @Override
    public double rangeRelSel(int type, int property, double min, double max) {
        final double numProps = stats.relWithPropertyNumeric(type, property);
        if (numProps <= 0) {
            return 0.0;
        }
        final double n = stats.numRelationships(type);
        if (min == max) {
            final double withProp = stats.relWithProperty(type, property);
            final double withPropUnique = stats.relWithPropertyUnique(type, property);
            final double hasProp = withProp / n;
            final double isNumeric = numProps / withProp;
            final double propHasValue = 1 / (isNumeric * withPropUnique);
            return hasProp * isNumeric * propHasValue;
        }
        return stats.relPropertyRange(type, property, min, max).orElse(0.0);
    }

    private double relCount(int labelAtBase, int type, int labelAtTarget) {
        return simulateNeo4j ? Math.min(stats.relCount(-1, type, labelAtTarget),
                stats.relCount(labelAtBase, type, -1))
                : stats.relCount(labelAtBase, type, labelAtTarget);
    }

    @Override
    public double relationships(int labelAtBase, int type, int labelAtTarget, Direction direction) {
        switch (direction) {
            case BOTH:
                return this.relCount(labelAtBase, type, labelAtTarget)
                        + this.relCount(labelAtTarget, type, labelAtBase);
            case INCOMING:
                return this.relCount(labelAtTarget, type, labelAtBase);
            case OUTGOING:
                return this.relCount(labelAtBase, type, labelAtTarget);
            default:
                throw new AssertionError();
        }
    }

    @Override
    public double nodes(int label) {
        return stats.numNodes(label);
    }
}
