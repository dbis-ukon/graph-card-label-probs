package kn.uni.dbis.alhd.queries;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CypherQuery {

    private final CypherPattern pattern;
    private final Map<String, Map<String, PropSelection>> nodePreds;
    private final Map<String, Map<String, PropSelection>> edgePreds;

    public static String serializeProperties(final Map<String, Map<String, Object>> properties) {
        return serializeProperties(properties, ", ");
    }

    public static String serializeProperties(final Map<String, Map<String, Object>> properties, final String sep) {
        final StringBuilder sb = new StringBuilder();
        for (final Map.Entry<String, Map<String, Object>> varE : properties.entrySet()) {
            final String vr = varE.getKey();
            for (final Map.Entry<String, Object> propE : varE.getValue().entrySet()) {
                final String prop = propE.getKey();
                final Object val = propE.getValue();
                if (sb.length() > 0) {
                    sb.append(sep);
                }
                sb.append("`").append(vr).append("`").append(".").append("`").append(prop).append("`").append("=");
                serializeValue(sb, val);
            }
        }
        return sb.toString();
    }

    private static void serializeValue(StringBuilder sb, Object val) {
        if (val instanceof Number) {
            sb.append(((Number) val).doubleValue());
        } else {
            final String repr = val.toString().replace("\\", "\\\\").replace("\"", "\\\"");
            sb.append('"').append(repr).append('"');
        }
    }

    public static Map<String, Map<String, Object>> parseProperties(final String propStr) {
        return parseProperties(propStr, ", ");
    }

    public static Map<String, Map<String, Object>> parseProperties(final String propStr, final String sep) {
        final Map<String, Map<String, Object>> props = new HashMap<>();
        int pos = 0;
        try {
            while (pos < propStr.length()) {
                pos++;
                final int next = propStr.indexOf("`.`", pos);
                final String vr = propStr.substring(pos, next);
                pos = next + 3;
                final int next2 = propStr.indexOf("`=", pos);
                final String key = propStr.substring(pos, next2);
                pos = next2 + 2;
                if (propStr.charAt(pos) == '"') {
                    pos++;
                    int end = propStr.indexOf("\"" + sep + "`", pos);
                    if (end < 0) {
                        end = propStr.length() - 1;
                    }
                    final String val = propStr.substring(pos, end)
                            .replace("\\\\", "\t")
                            .replace("\\", "\"")
                            .replace("\t", "\\");
                    props.computeIfAbsent(vr, k -> new HashMap<>()).put(key, val);
                    pos = end + 1;
                } else {
                    final int next3 = propStr.indexOf(", ", pos);
                    final double val;
                    if (next3 >= 0) {
                        val = Double.parseDouble(propStr.substring(pos, next3));
                        pos = next3;
                    } else {
                        val = Double.parseDouble(propStr.substring(pos));
                        pos = propStr.length();
                    }
                    props.computeIfAbsent(vr, k -> new HashMap<>()).put(key, val);

                }
                if (pos < propStr.length()) {
                    pos += 2;
                }
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(propStr);
            throw e;
        }
        return props;
    }


    public CypherQuery(final CypherPattern pattern,
                       final Map<String, Map<String, PropSelection>> nodePreds,
                       final Map<String, Map<String, PropSelection>> edgePreds) {
        this.pattern = pattern;
        this.nodePreds = nodePreds;
        this.edgePreds = edgePreds;
    }


    public CypherQuery(final CypherPattern pattern) {
        this(pattern, Collections.emptyMap(), Collections.emptyMap());
    }

    public static CypherQuery from(final String patternStr, final String props) {
        final CypherPattern pattern = CypherPattern.fromCypher(patternStr);
        final Map<String, Map<String, PropSelection>> nodePreds = new HashMap<>();
        final Map<String, Map<String, PropSelection>> edgePreds = new HashMap<>();
        for (final Map.Entry<String, Map<String, Object>> e : parseProperties(props).entrySet()) {
            final String vr = e.getKey();
            final Map<String, Map<String, PropSelection>> preds =
                    pattern.getNodeVars().containsKey(vr) ? nodePreds : edgePreds;
            final Map<String, PropSelection> curr = new HashMap<>();
            preds.put(vr, curr);
            for (final Map.Entry<String, Object> e2 : e.getValue().entrySet()) {
                final String prop = e2.getKey();
                final Object val = e2.getValue();
                if (val instanceof Number) {
                    curr.put(prop, new PropSelection(prop, "=", ((Number) val).doubleValue()));
                } else {
                    curr.put(prop, new PropSelection(prop, val.toString()));
                }
            }
        }
        return new CypherQuery(pattern, nodePreds, edgePreds);
    }

    public String toCypher(final boolean homomorphism) {
        if (nodePreds.isEmpty() && edgePreds.isEmpty()) {
            return "MATCH " + pattern.toString();
        }
        final Map<Set<String>, String> edgeNames = edgePreds.keySet().stream()
                .filter(e -> e.contains("_"))
                .collect(Collectors.toMap(e -> new HashSet<>(Arrays.asList(e.split("_"))), e -> e));

        final StringBuilder sb = new StringBuilder(pattern.toMatchClauses(edgeNames, homomorphism));
        boolean first = true;
        for (final Map<String, Map<String, PropSelection>> m : Arrays.asList(nodePreds, edgePreds)) {
            for (final Map.Entry<String, Map<String, PropSelection>> e : m.entrySet()) {
                final String var = e.getKey();
                for (final Map.Entry<String, PropSelection> sel : e.getValue().entrySet()) {
                    sb.append(first ? " WHERE " : " AND ");
                    sel.getValue().toString(sb, var);
                    first = false;
                }
            }
        }
        return sb.toString();
    }

    public Collection<CypherQuery> subQueries(final int k) {
        final List<String> nodes = new ArrayList<>(this.pattern.getNodeVars().keySet());
        Collections.sort(nodes);
        if (k >= nodes.size()) {
           return k == nodes.size() ? List.of(this) : List.of();
        }
        final Map<String, Integer> nodeToPos = IntStream.range(0, nodes.size()).boxed()
                .collect(Collectors.toMap(nodes::get, i -> i));
        final Set<BitSet> out = new HashSet<>();
        final int limit = nodes.size() - k;
        final BitSet vars = new BitSet();
        for (int i = 0; i < limit; i++) {
            vars.set(i);
            subQueriesFrom(nodeToPos, k, vars, out::add);
            vars.clear(i);
        }
        return out.stream().map(vs -> {
            final Map<String, Set<String>> newNodes = vs.stream().mapToObj(nodes::get)
                    .collect(Collectors.toMap(n -> n, this.pattern.getNodeVars()::get));
            final List<CypherPattern.Relationship> relationships = this.pattern.getRelationships().stream()
                    .filter(e -> newNodes.containsKey(e.getSource()) && newNodes.containsKey(e.getTarget()))
                    .collect(Collectors.toList());
            if (relationships.isEmpty()) {
                throw new AssertionError();
            }
            final Set<String> edgeNames = relationships.stream()
                    .map(CypherPattern.Relationship::name).collect(Collectors.toSet());
            final CypherPattern pat = new CypherPattern(newNodes, relationships);
            final Map<String, Map<String, PropSelection>> nodePreds = this.nodePreds.entrySet().stream()
                    .filter(e -> newNodes.containsKey(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            final Map<String, Map<String, PropSelection>> edgePreds = this.edgePreds.entrySet().stream()
                    .filter(e -> edgeNames.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return new CypherQuery(pat, nodePreds, edgePreds);
        }).collect(Collectors.toList());
    }

    public Optional<String[][]> toTriples(final boolean rdfType) {
        if (this.edgePreds.values().stream().anyMatch(m -> !m.isEmpty())) {
            return Optional.empty();
        }
        final var patternTrips = this.pattern.toTriples(rdfType);
        if (patternTrips.isEmpty()) {
            return patternTrips;
        }
        final List<String[]> outTrips = new ArrayList<>(Arrays.asList(patternTrips.get()));
        for (final var e : this.nodePreds.entrySet()) {
            final String nv = e.getKey();
            for (final var e2 : e.getValue().entrySet()) {
                final var pred = e2.getValue().valueString();
                if (pred.isEmpty()) {
                    return Optional.empty();
                }
                outTrips.add(new String[] {"?" + nv, "P:" + e2.getKey(), pred.get()});
            }
        }
        return Optional.of(outTrips.toArray(String[][]::new));
    }

    private void subQueriesFrom(final Map<String, Integer> nodeToPos,
                                final int k, final BitSet vars, final Consumer<BitSet> out) {
        if (vars.cardinality() == k) {
            out.accept((BitSet) vars.clone());
            return;
        }
        final BitSet neighborhood = new BitSet();
        for (final CypherPattern.Relationship rel : this.getPattern().getRelationships()) {
            final int fromID = nodeToPos.get(rel.getSource());
            final int toID = nodeToPos.get(rel.getTarget());
            final boolean fromIn = vars.get(fromID);
            final boolean toIn = vars.get(toID);
            if (fromIn ^ toIn) {
                neighborhood.set(fromIn ? toID : fromID);
            }
        }
        neighborhood.stream().forEach(v -> {
            vars.set(v);
            subQueriesFrom(nodeToPos, k, vars, out);
            vars.clear(v);
        });
    }

    public CypherPattern getPattern() {
        return this.pattern;
    }

    public Map<String, Map<String, PropSelection>> getNodePredicates() {
        return this.nodePreds;
    }

    public Map<String, Map<String, PropSelection>> getEdgePredicates() {
        return this.edgePreds;
    }

    public int size() {
        return this.pattern.numRelationships() + this.pattern.numNodeLabels()
                + this.nodePreds.values().stream().mapToInt(Map::size).sum()
                + this.edgePreds.values().stream().mapToInt(Map::size).sum();
    }
}
