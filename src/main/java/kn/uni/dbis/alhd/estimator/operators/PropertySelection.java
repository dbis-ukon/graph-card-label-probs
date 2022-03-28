package kn.uni.dbis.alhd.estimator.operators;

import kn.uni.dbis.alhd.estimator.GADbProperties;
import kn.uni.dbis.alhd.estimator.GAResultProperties;
import kn.uni.dbis.alhd.queries.PropSelection;

import java.util.*;

/**
 * Implementation of the PropertySelection-operator.
 *
 * The property selection keeps all subgraphs where the node or relationship matched by
 * the given selection variable has the given property with the given value.
 */
public class PropertySelection extends Selection {

    /** The selection variable. */
    private final String variable;

    /** The predicates. */
    private final Map<Integer, PropSelection> predicates;

    private final Double staticSelectivity;

    /**
     * Constructs a new property selection operator.
     *
     * @param dbProps database properties
     * @param variable the nodes or relationships to select from
     * @param predicates selection predicates
     */
    public PropertySelection(final GADbProperties dbProps, final String variable,
                             final Map<Integer, PropSelection> predicates,
                             final Double staticSelectivity) {
        super(dbProps);
        this.variable = variable;
        if (predicates.isEmpty()) {
            throw new IllegalArgumentException();
        }
        this.predicates = new TreeMap<>(predicates);
        this.staticSelectivity = staticSelectivity;
    }

    /**
     * Returns the selection variable.
     *
     * @return the variable used for the selection
     */
    public String getVariable() {
        return this.variable;
    }

    public Map<Integer, PropSelection> getPredicates() {
        return this.predicates;
    }

    @Override
    public Set<String> getIncludedVariables() {
        return Collections.singleton(this.variable);
    }

    @Override
    public GAResultProperties computeLogicalProperties(final List<GAResultProperties> inputProperties) {
        final GAResultProperties input = inputProperties.get(0);
        if (input.anyNodeMatchedBy(this.variable)) {
            return nodeSelectivity(input);
        } else if (input.anyRelationshipMatchedBy(this.variable)) {
            return relationshipSelectivity(input);
        } else {
            throw new IllegalStateException("Selection variable is not matched.");
        }
    }

    private GAResultProperties relationshipSelectivity(final GAResultProperties input) {
        final GADbProperties dbProps = this.getDBProperties();
        final Set<Integer> types = input.getRelationshipTypeMap().get(this.variable);
        if (types.isEmpty()) {
            throw new IllegalStateException("No relationship types allowed.");
        }

        final double selectivity;
        final Set<Integer> remainingTypes = new HashSet<>();
        if (staticSelectivity != null) {
            selectivity = Math.pow(staticSelectivity, this.predicates.size());
            remainingTypes.addAll(types);
        } else {
            double sel = 1.0;
            for (final Map.Entry<Integer, PropSelection> e : this.predicates.entrySet()) {
                double selSum = 0.0;
                final int property = e.getKey();
                final PropSelection pred = e.getValue();
                for (final Integer t : types) {
                    final double s;
                    if (pred.value != null) {
                        s = dbProps.relSel(t, property, pred.value.hashCode());
                    } else if (pred.range[0] == pred.range[1]) {
                        s = dbProps.relSel(t, property, Double.hashCode(pred.range[0]));
                    } else {
                        final double[] range = pred.range;
                        s = dbProps.rangeRelSel(t, property, range[0], range[1]);
                    }
                    if (s > 0) {
                        remainingTypes.add(t);
                    }
                    selSum += s;
                }
                sel = Math.min(sel, selSum / types.size());
            }
            selectivity = sel;
        }
        final Map<String, Set<Integer>> relTypeMap = new HashMap<>(input.getRelationshipTypeMap());
        relTypeMap.put(this.variable, remainingTypes);
        return new GAResultProperties(input.getNodeLabelMap(), relTypeMap,input.getSize() * selectivity, false);
    }

    private GAResultProperties nodeSelectivity(final GAResultProperties input) {
        final int[] nodeLabels = input.getLabels(this.variable).entrySet().stream()
                .filter(e -> e.getValue() > 0).mapToInt(Map.Entry::getKey).toArray();
        final GADbProperties dbProps = this.getDBProperties();
        final double selectivity;
        final Set<Integer> labels = new HashSet<>();
        if (nodeLabels.length == 0) {
            selectivity = Math.pow(0.1, this.predicates.size());
        } else if (staticSelectivity != null) {
            selectivity = Math.pow(this.staticSelectivity, this.predicates.size());
        } else {
            double sel = 1.0;
            for (final Map.Entry<Integer, PropSelection> e : this.predicates.entrySet()) {
                final int property = e.getKey();
                final PropSelection pred = e.getValue();
                double selSum = 0.0;
                for (final Integer l : nodeLabels) {
                    final double s;
                    if (pred.value != null) {
                        s = dbProps.sel(l, property, pred.value.hashCode());
                    } else if (pred.range[0] == pred.range[1]) {
                        s = dbProps.sel(l, property, Double.hashCode(pred.range[0]));
                    } else {
                        final double[] range = pred.range;
                        s = dbProps.rangeSel(l, property, range[0], range[1]);
                    }
                    selSum += s;
                    if (s > 0) {
                        labels.add(l);
                    }
                }
                sel = Math.min(sel, selSum / nodeLabels.length);
            }
            selectivity = sel;
        }

        final Map<String, Map<Integer, Double>> nodeLabelMap = new HashMap<>(input.getNodeLabelMap());
        final Map<Integer, Double> oldFractions = nodeLabelMap.get(this.variable);
        if (selectivity == 0) {
            return new GAResultProperties(nodeLabelMap, input.getRelationshipTypeMap(), 0, false);
        }
        final Map<Integer, Double> newFractions = new HashMap<>(oldFractions);
        for (final var e : newFractions.entrySet()) {
            if (labels.contains(e.getKey())) {
                e.setValue(Math.min(e.getValue() / selectivity, 1.0d));
            } else {
                e.setValue(0.0);
            }
        }
        nodeLabelMap.put(this.variable, newFractions);
        return new GAResultProperties(nodeLabelMap, input.getRelationshipTypeMap(),
                selectivity * input.getSize(), false);
    }

    @Override
    public int hash() {
        return Objects.hash(this.variable, this.predicates);
    }

    @Override
    public boolean eq(final GALogicalOperator other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PropertySelection)) {
            return false;
        }
        final PropertySelection otherLabelSelection = (PropertySelection) other;
        return otherLabelSelection.getVariable().equals(this.variable)
                && otherLabelSelection.getPredicates().equals(this.predicates);
    }

    @Override
    public String toString() {
        return "PropertySelection[" +
                "variable='" + variable + '\'' +
                ", predicates=" + predicates +
                ", staticSelectivity=" + staticSelectivity +
                ']';
    }
}
