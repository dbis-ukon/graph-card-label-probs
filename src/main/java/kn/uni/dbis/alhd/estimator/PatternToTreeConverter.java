package kn.uni.dbis.alhd.estimator;

import kn.uni.dbis.alhd.estimator.operators.*;
import kn.uni.dbis.alhd.queries.CypherPattern;
import kn.uni.dbis.alhd.queries.CypherQuery;
import kn.uni.dbis.alhd.queries.Direction;
import kn.uni.dbis.alhd.queries.PropSelection;
import kn.uni.dbis.alhd.statistics.GraphStatistics;
import kn.uni.dbis.alhd.util.UnionFind;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PatternToTreeConverter {
	/**
	 * Maps the AST to an algebra expression that is the input of Cascades.
	 */
	public static Expression mapToAlgebraExpression(final GraphStatistics stats, final GADbProperties dbProps,
													final CypherPattern pattern, final boolean useNewJoin,
													final Double staticSelectivity) {
		return mapToAlgebraExpression(stats, dbProps,
				new CypherQuery(pattern, Collections.emptyMap(), Collections.emptyMap()), useNewJoin, staticSelectivity);
	}

	/**
	 * Maps the AST to an algebra expression that is the input of Cascades.
	 */
	public static Expression mapToAlgebraExpression(final GraphStatistics stats, final GADbProperties dbProps,
													final CypherQuery query, final boolean useNewJoin,
													final Double staticSelectivity) {
		final CypherPattern pattern = query.getPattern();
		final Map<String, Set<String>> nodeVars = pattern.getNodeVars();
		final List<CypherPattern.Relationship> rels = pattern.getRelationships();

		final Map<String, Integer> propIDs = stats.getPropertyIDs();
		final Map<String, Map<String, PropSelection>> allNodePreds = query.getNodePredicates();
		final Map<String, Map<String, PropSelection>> allRelPreds = query.getEdgePredicates();

		// lookup function for both node (`true`) and relationship (`false`) property predicates
		final BiFunction<String, Boolean, Map<Integer, PropSelection>> propertyPreds = (key, nodeVar) -> {
			final Map<String, PropSelection> preds = (nodeVar ? allNodePreds : allRelPreds).get(key);
			return preds == null ? Collections.emptyMap() : preds.entrySet().stream()
					.collect(Collectors.toMap(et -> propIDs.get(et.getKey()), Entry::getValue));
		};

		final AtomicInteger varGen = new AtomicInteger();

		// efficient lookup from variable name to position in the array
		final Map<String, Integer> varToPos = new HashMap<>();
		final String[] nodesArr = nodeVars.keySet().toArray(String[]::new);
		for (int i = 0; i < nodesArr.length; i++) {
			varToPos.put(nodesArr[i], i);
		}

		// compute weakly connected components of the pattern
		final BitSet[] expandLookup = IntStream.range(0, nodesArr.length)
				.mapToObj(i -> new BitSet(rels.size())).toArray(BitSet[]::new);
		final int[] unionFind = IntStream.range(0, nodesArr.length).map(i -> -1).toArray();
		for (int i = 0; i < rels.size(); i++) {
			final CypherPattern.Relationship expand = rels.get(i);
			for (final String v : Arrays.asList(expand.getSource(), expand.getTarget())) {
				expandLookup[varToPos.get(v)].set(i);
			}
			UnionFind.union(unionFind, varToPos.get(expand.getSource()), varToPos.get(expand.getTarget()));
		}

		// gather nodes and relationships contained in each component
		final Map<Integer, Set<String>> comps = new HashMap<>();
		for (int i = 0; i < unionFind.length; i++) {
			final int compID = UnionFind.find(unionFind, i);
			comps.computeIfAbsent(compID, k -> new HashSet<>()).add(nodesArr[i]);
		}
		final Map<Integer, BitSet> compToRel = new HashMap<>();
		for (int i = 0; i < rels.size(); i++) {
			final CypherPattern.Relationship rel = rels.get(i);
			final int compID = UnionFind.find(unionFind, varToPos.get(rel.getSource()));
			compToRel.computeIfAbsent(compID, k -> new BitSet()).set(i);
		}

		// go through all connected components and build up the operator trees
		final List<Expression> components = new ArrayList<>();
		for (final Entry<Integer, Set<String>> e : comps.entrySet()) {
			final int compID = e.getKey();
			final Set<String> compNodeVars = e.getValue();
			final BitSet nodesSeen = new BitSet();

			// initial node in the component, chosen as the one with the most neighbors
			final int startNode = compNodeVars.stream().map(varToPos::get)
					.max(Comparator.comparing(vid -> expandLookup[vid].cardinality())).orElseThrow();
			nodesSeen.set(startNode);

			// expression that is extended
			final String firstNodeName = nodesArr[startNode];
			Expression compExpr = addNodeSelections(stats, nodeVars, propertyPreds.apply(firstNodeName, true), dbProps,
					firstNodeName, new Expression(new GetNodes(dbProps, firstNodeName)), staticSelectivity);

			final BitSet relSet = compToRel.get(compID);
			if (relSet == null) {
				// no relationships in this pattern
				components.add(compExpr);
				continue;
			}

			final Deque<CypherPattern.Relationship> relDeque = new ArrayDeque<>();
			final List<CypherPattern.Relationship> deferred = new ArrayList<>();
			final BitSet firstEdges = expandLookup[startNode];
			final BitSet edgesPending = (BitSet) relSet.clone();
			edgesPending.andNot(firstEdges);
			firstEdges.stream().mapToObj(rels::get).forEach(relDeque::addLast);

			// add one relationship and adjacent node variable at a time
			while (!relDeque.isEmpty()) {
				final CypherPattern.Relationship next = relDeque.pollFirst();
				final String eName = next.name();
				final int fromID = varToPos.get(next.getSource());
				final int toID = varToPos.get(next.getTarget());

				String newVar = null;
				if (nodesSeen.get(fromID)) {
					if (nodesSeen.get(toID)) {
						// relationship closes a cycle
						deferred.add(next);
					} else {
						// relationship is outgoing
						final Expand expand = toExpand(stats, dbProps, next, true, varGen);
						final Map<Integer, PropSelection> preds = propertyPreds.apply(eName, false);
						final Expression input = new Expression(expand, compExpr);
						compExpr = preds.isEmpty() ? input : new Expression(
								new PropertySelection(dbProps, expand.getRelationshipVariable(), preds, staticSelectivity), input);
						newVar = next.getTarget();
					}
				} else if (nodesSeen.get(toID)) {
					// relationship is incoming
					final Expand expand = toExpand(stats, dbProps, next, false, varGen);
					final Map<Integer, PropSelection> preds = propertyPreds.apply(eName, false);
					final Expression input = new Expression(expand, compExpr);
					compExpr = preds.isEmpty() ? input : new Expression(
							new PropertySelection(dbProps, expand.getRelationshipVariable(), preds, staticSelectivity), input);
					newVar = next.getSource();
				} else {
					throw new AssertionError();
				}

				if (newVar != null) {
					// add new variable's label restrictions
					final int nvID = varToPos.get(newVar);
					compExpr = addNodeSelections(stats, nodeVars, propertyPreds.apply(newVar, true),
							dbProps, newVar, compExpr, staticSelectivity);
					expandLookup[nvID].stream().filter(edgesPending::get).forEach(vid -> {
						edgesPending.clear(vid);
						relDeque.addLast(rels.get(vid));
					});
					nodesSeen.set(nvID);
				}
			}
			for (final CypherPattern.Relationship next : deferred) {
				final String temp = "$v" + varGen.getAndIncrement();
				final Expand expand = toExpand(stats, dbProps, next.redirect(temp), true, varGen);
				compExpr = new Expression(expand, compExpr);
				final Map<Integer, PropSelection> preds = propertyPreds.apply(next.name(), false);
				if (!preds.isEmpty()) {
					final PropertySelection sel = new PropertySelection(dbProps, expand.getRelationshipVariable(), preds, staticSelectivity);
					compExpr = new Expression(sel, compExpr);
				}
				compExpr = new Expression(useNewJoin ? new MergeOn(dbProps, next.getTarget(), temp)
						: new SelfJoin(dbProps, next.getTarget(), temp), compExpr);
			}
			components.add(compExpr);
		}

		//	We have to join all of the found components.
		Expression joinedExp = null;
		for (final Expression e : components) {
			final GALogicalOperator op = new NodeJoin(dbProps);
			joinedExp = joinedExp == null ? e : new Expression(op, joinedExp, e);
		}

//		System.out.println(joinedExp);
		return joinedExp;
	}

	private static Expand toExpand(GraphStatistics stats, GADbProperties dbProps, CypherPattern.Relationship edge,
								   final boolean out, AtomicInteger varGen) {
		final Optional<String> tp = edge.getType();
		final Set<Integer> types = Collections.singleton(tp.map(stats::getTypeID).map(OptionalInt::orElseThrow).orElse(-1));
		final String relVar = "$e" + varGen.getAndIncrement();
		if (out) {
			return new Expand(dbProps, edge.getSource(), edge.isDirected() ? Direction.OUTGOING : Direction.BOTH, relVar, types, edge.getTarget());
		} else {
			return new Expand(dbProps, edge.getTarget(), edge.isDirected() ? Direction.INCOMING : Direction.BOTH, relVar, types, edge.getSource());
		}
	}

	private static Expression addNodeSelections(final GraphStatistics stats, final Map<String, Set<String>> nodeVars,
			final Map<Integer, PropSelection> preds, final GADbProperties dbProps, final String var, final Expression input,
												final Double staticSelectivity) {
		Expression expr = input;
		for (final String lbl : nodeVars.get(var)) {
			final GALogicalOperator op = new NodeLabelSelection(dbProps, var, stats.getLabelID(lbl).orElseThrow());
			expr = new Expression(op, expr);
		}
		return preds.isEmpty() ? expr : new Expression(new PropertySelection(dbProps, var, preds, staticSelectivity), expr);
	}
}
