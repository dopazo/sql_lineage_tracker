import dagre from "@dagrejs/dagre";
import type { Node, Edge } from "@xyflow/react";

const NODE_WIDTH = 280;
const NODE_HEIGHT_BASE = 60;
const NODE_HEIGHT_PER_COLUMN = 24;

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
    const columnCount = (node.data.columnCount as number) ?? 0;
    const height = NODE_HEIGHT_BASE + columnCount * NODE_HEIGHT_PER_COLUMN;
    g.setNode(node.id, { width: NODE_WIDTH, height });
  }

  for (const edge of edges) {
    g.setEdge(edge.source, edge.target);
  }

  dagre.layout(g);

  const layoutedNodes = nodes.map((node) => {
    const pos = g.node(node.id);
    const columnCount = (node.data.columnCount as number) ?? 0;
    const height = NODE_HEIGHT_BASE + columnCount * NODE_HEIGHT_PER_COLUMN;
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
