package kn.uni.dbis.alhd.estimator.operators;

import kn.uni.dbis.alhd.estimator.GADbProperties;
import kn.uni.dbis.alhd.estimator.GAResultProperties;
import kn.uni.dbis.alhd.estimator.LabelDistribution;

import java.util.*;

/**
 * Implementation of the NodeLabelSelection-Operator.
 *
 * The node label selection keeps all subgraphs from the input, where the node matched by
 * the given variable has all of the given labels.
 */
public class NodeLabelSelection extends Selection {

    /** The selection variable. */
    private final String variable;

    /** The node label nodes matched by the selection variable must have. */
    private final int wantedLabel;

    /**
     * Constructs a new NodeLabelSelection-operator.
     *
     * @param dbProps database properties
     * @param variable the nodes to select from
     * @param wantedLabel which label these nodes must have
     */
    public NodeLabelSelection(final GADbProperties dbProps, final String variable, final int wantedLabel) {
        super(dbProps);
        this.variable = variable;
        this.wantedLabel = wantedLabel;
    }

    /**
     * Returns the selection variable.
     *
     * @return the variable used for the selection
     */
    public String getVariable() {
        return variable;
    }

    /**
     * Returns the label that nodes matched by the selection variable must have,
     * in order to be part of the result.
     *
     * @return the label wanted by the selection
     */
    public int getWantedLabel() {
        return wantedLabel;
    }

    @Override
    public Set<String> getIncludedVariables() {
        return Collections.singleton(this.variable);
    }

    @Override
    public GAResultProperties computeLogicalProperties(final List<GAResultProperties> inputProperties) {
        final GADbProperties dbProps = this.getDBProperties();
        final GAResultProperties input = inputProperties.get(0);
        final LabelDistribution labelDist = dbProps.getLabelDistribution();

        // The reduction is given by the node label map.
        final Map<String, Map<Integer, Double>> nodeLabelMap = new HashMap<>(input.getNodeLabelMap());
        final Map<Integer, Double> oldFractions = nodeLabelMap.get(this.variable);
        final double reduction = oldFractions.getOrDefault(this.wantedLabel, 0.0);

        if (reduction == 0.0d) {
            return new GAResultProperties(nodeLabelMap, input.getRelationshipTypeMap(), 0, false);
        } else {
            final Map<Integer, Double> newFractions = new HashMap<>(oldFractions);
            for (Set<Integer> overlapping : labelDist.getLabelHierarchy()) {
                if (overlapping.contains(this.wantedLabel)) {
                    for (int l : overlapping) {
                        if (l == this.wantedLabel || labelDist.getAllKnownSublabels(l).contains(this.wantedLabel)) {
                            // update fractions of all superlabels
                            newFractions.put(l, 1.0d);
                        } else if (labelDist.getAllKnownSublabels(this.wantedLabel).contains(l)) {
                            // update fractions of sublabels
                            newFractions.put(l, Math.min(oldFractions.get(l) / reduction, 1.0d));
                        }
                    }
                    // assumption: the fractions for all labels which overlap with the new certain label but are not
                    // known to be sub- or superlabels remain the same.
                } else {
                    // nodes with labels from the current set of overlapping labels are disjunct from the selected
                    // nodes.
                    for (int l : overlapping) {
                        newFractions.put(l, 0.0d);
                    }
                }
            }
            nodeLabelMap.put(this.variable, newFractions);

            return new GAResultProperties(nodeLabelMap, input.getRelationshipTypeMap(),
                    input.getSize() * reduction, false);
        }
    }

    @Override
    public int hash() {
        return Objects.hash(this.variable, this.wantedLabel);
    }

    @Override
    public boolean eq(final GALogicalOperator other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof NodeLabelSelection)) {
            return false;
        }
        final NodeLabelSelection otherLabelSelection = (NodeLabelSelection) other;
        return otherLabelSelection.getVariable().equals(this.variable)
                && otherLabelSelection.getWantedLabel() == this.wantedLabel;
    }

    @Override
    public String toString() {
        return "Selection_" + this.getVariable() + ":" + this.wantedLabel;
    }
}
