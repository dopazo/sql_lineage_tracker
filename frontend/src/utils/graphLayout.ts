import dagre from "@dagrejs/dagre";
import type { Node, Edge } from "@xyflow/react";

const NODE_WIDTH = 280;
const NODE_HEIGHT_BASE = 60;
const NODE_HEIGHT_PER_COLUMN = 24;

function getNodeHeight(node: Node): number {
  const data = node.data as Record<string, unknown>;
  const lineageNode = data.lineageNode as Record<string, unknown> | undefined;
  const columns = lineageNode?.columns;
  const count = Array.isArray(columns) ? columns.length : 0;
  return NODE_HEIGHT_BASE + count * NODE_HEIGHT_PER_COLUMN;
}

export function getLayoutedElements(
  nodes: Node[],
  edges: Edge[],
  direction: "LR" | "TB" = "LR"
): { nodes: Node[]; edges: Edge[] } {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({
    rankdir: direction,
    nodesep: 50,
    ranksep: 120,
    edgesep: 20,
  });

  for (const node of nodes) {
    g.setNode(node.id, { width: NODE_WIDTH, height: getNodeHeight(node) });
  }

  for (const edge of edges) {
    g.setEdge(edge.source, edge.target);
  }

  dagre.layout(g);

  const layoutedNodes = nodes.map((node) => {
    const pos = g.node(node.id);
    const height = getNodeHeight(node);
    return {
      ...node,
      position: {
        x: pos.x - NODE_WIDTH / 2,
        y: pos.y - height / 2,
      },
    };
  });

  return { nodes: layoutedNodes, edges };
}
