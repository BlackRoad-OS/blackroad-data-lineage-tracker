"""
Data Lineage Tracking System
Tracks data transformations and dependencies (similar to dbt lineage) with SQLite persistence.
"""

import sqlite3
import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Literal
import json

# Database location
DB_PATH = Path.home() / ".blackroad" / "lineage.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


@dataclass
class LineageNode:
    id: str
    name: str
    type: str  # source, transform, model, view, mart, export
    source: str = ""
    schema: str = ""
    description: str = ""
    created_at: float = 0.0


@dataclass
class LineageEdge:
    from_id: str
    to_id: str
    transformation: str = ""
    created_at: float = 0.0


class DataLineageTracker:
    def __init__(self):
        self.conn = sqlite3.connect(str(DB_PATH))
        self.cursor = self.conn.cursor()
        self._init_db()

    def _init_db(self):
        """Initialize database schema."""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                type TEXT NOT NULL,
                source TEXT,
                schema TEXT,
                description TEXT,
                created_at REAL
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS edges (
                from_id TEXT,
                to_id TEXT,
                transformation TEXT,
                created_at REAL,
                PRIMARY KEY (from_id, to_id),
                FOREIGN KEY (from_id) REFERENCES nodes(id),
                FOREIGN KEY (to_id) REFERENCES nodes(id)
            )
        """)
        self.conn.commit()

    def register_source(self, name: str, type_: str, connection_string: str = "", description: str = "") -> LineageNode:
        """Register a source/sink/transform node."""
        now = datetime.now().timestamp()
        node_id = f"{type_}_{name.lower().replace(' ', '_')}"
        
        try:
            self.cursor.execute(
                """INSERT INTO nodes (id, name, type, source, description, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (node_id, name, type_, connection_string, description, now)
            )
            self.conn.commit()
            return LineageNode(node_id, name, type_, connection_string, "", description, now)
        except sqlite3.IntegrityError:
            return self.get_node_by_name(name)

    def get_node_by_name(self, name: str) -> Optional[LineageNode]:
        """Retrieve node by name."""
        self.cursor.execute("SELECT * FROM nodes WHERE name = ?", (name,))
        row = self.cursor.fetchone()
        if row:
            return LineageNode(*row)
        return None

    def get_node(self, node_id: str) -> Optional[LineageNode]:
        """Retrieve node by ID."""
        self.cursor.execute("SELECT * FROM nodes WHERE id = ?", (node_id,))
        row = self.cursor.fetchone()
        if row:
            return LineageNode(*row)
        return None

    def register_transform(self, name: str, inputs: List[str], outputs: List[str], sql_or_code: str = "") -> LineageNode:
        """Register a transformation with inputs and outputs."""
        now = datetime.now().timestamp()
        node_id = f"transform_{name.lower().replace(' ', '_')}"
        
        try:
            self.cursor.execute(
                """INSERT INTO nodes (id, name, type, source, description, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (node_id, name, "transform", sql_or_code, "", now)
            )
            
            # Create edges from inputs to this node
            for input_name in inputs:
                input_node = self.get_node_by_name(input_name)
                if input_node:
                    self.cursor.execute(
                        """INSERT INTO edges (from_id, to_id, transformation, created_at)
                           VALUES (?, ?, ?, ?)""",
                        (input_node.id, node_id, sql_or_code, now)
                    )
            
            # Create edges from this node to outputs
            for output_name in outputs:
                output_node = self.get_node_by_name(output_name)
                if output_node:
                    self.cursor.execute(
                        """INSERT INTO edges (from_id, to_id, transformation, created_at)
                           VALUES (?, ?, ?, ?)""",
                        (node_id, output_node.id, sql_or_code, now)
                    )
            
            self.conn.commit()
            return LineageNode(node_id, name, "transform", sql_or_code, "", "", now)
        except sqlite3.IntegrityError:
            return self.get_node_by_name(name)

    def get_lineage(self, node_name: str, direction: str = "both") -> Dict:
        """Get upstream/downstream/both lineage for a node."""
        node = self.get_node_by_name(node_name)
        if not node:
            return {"error": f"Node {node_name} not found"}
        
        upstream = []
        downstream = []
        
        if direction in ["upstream", "both"]:
            upstream = self._get_upstream(node.id)
        
        if direction in ["downstream", "both"]:
            downstream = self._get_downstream(node.id)
        
        return {
            "node": node_name,
            "type": node.type,
            "upstream": upstream,
            "downstream": downstream
        }

    def _get_upstream(self, node_id: str, visited=None) -> List[str]:
        """Recursively get all upstream nodes."""
        if visited is None:
            visited = set()
        
        if node_id in visited:
            return []
        visited.add(node_id)
        
        self.cursor.execute(
            "SELECT from_id FROM edges WHERE to_id = ?",
            (node_id,)
        )
        parents = []
        for row in self.cursor.fetchall():
            parent_id = row[0]
            parent_node = self.get_node(parent_id)
            if parent_node:
                parents.append(parent_node.name)
                parents.extend(self._get_upstream(parent_id, visited))
        
        return list(set(parents))

    def _get_downstream(self, node_id: str, visited=None) -> List[str]:
        """Recursively get all downstream nodes."""
        if visited is None:
            visited = set()
        
        if node_id in visited:
            return []
        visited.add(node_id)
        
        self.cursor.execute(
            "SELECT to_id FROM edges WHERE from_id = ?",
            (node_id,)
        )
        children = []
        for row in self.cursor.fetchall():
            child_id = row[0]
            child_node = self.get_node(child_id)
            if child_node:
                children.append(child_node.name)
                children.extend(self._get_downstream(child_id, visited))
        
        return list(set(children))

    def visualize_ascii(self, node_name: str) -> str:
        """Generate ASCII tree visualization of dependencies."""
        node = self.get_node_by_name(node_name)
        if not node:
            return f"Node {node_name} not found"
        
        lines = []
        lines.append(f"📊 Lineage for: {node_name} ({node.type})")
        lines.append("─" * 50)
        
        upstream = self._get_upstream(node.id)
        if upstream:
            lines.append("📥 Upstream Dependencies:")
            for dep in sorted(upstream):
                lines.append(f"  ├─ {dep}")
        
        downstream = self._get_downstream(node.id)
        if downstream:
            lines.append("📤 Downstream Dependencies:")
            for dep in sorted(downstream):
                lines.append(f"  ├─ {dep}")
        
        if not upstream and not downstream:
            lines.append("  (No dependencies)")
        
        return "\n".join(lines)

    def impact_analysis(self, node_name: str) -> Dict:
        """Analyze what breaks if this node changes."""
        node = self.get_node_by_name(node_name)
        if not node:
            return {"error": f"Node {node_name} not found"}
        
        impacted = self._get_downstream(node.id)
        
        return {
            "node": node_name,
            "change_type": f"Modifying {node.type}",
            "directly_impacted": impacted,
            "impact_count": len(impacted),
            "severity": "high" if len(impacted) > 5 else "medium" if len(impacted) > 0 else "low"
        }

    def find_orphans(self) -> List[str]:
        """Find nodes with no connections."""
        self.cursor.execute("""
            SELECT id, name FROM nodes
            WHERE id NOT IN (
                SELECT from_id FROM edges UNION SELECT to_id FROM edges
            )
        """)
        return [row[1] for row in self.cursor.fetchall()]

    def export_dot(self, output_path: str):
        """Export graph as GraphViz .dot format."""
        self.cursor.execute("SELECT * FROM nodes")
        nodes = self.cursor.fetchall()
        
        self.cursor.execute("SELECT * FROM edges")
        edges = self.cursor.fetchall()
        
        with open(output_path, "w") as f:
            f.write("digraph lineage {\n")
            f.write("  rankdir=LR;\n")
            f.write("  node [shape=box, style=filled, fillcolor=lightblue];\n\n")
            
            # Write nodes with different colors by type
            color_map = {
                "source": "lightgreen",
                "transform": "lightyellow",
                "model": "lightcyan",
                "view": "lightpink",
                "mart": "lightgray",
                "export": "lightcoral"
            }
            
            for node_row in nodes:
                node_id, name = node_row[0], node_row[1]
                node_type = node_row[2]
                color = color_map.get(node_type, "lightblue")
                f.write(f'  "{name}" [label="{name}", fillcolor="{color}"];\n')
            
            f.write("\n")
            
            # Write edges
            for edge_row in edges:
                from_id, to_id = edge_row[0], edge_row[1]
                from_node = next((n[1] for n in nodes if n[0] == from_id), None)
                to_node = next((n[1] for n in nodes if n[0] == to_id), None)
                if from_node and to_node:
                    f.write(f'  "{from_node}" -> "{to_node}";\n')
            
            f.write("}\n")

    def stats(self) -> Dict:
        """Get lineage statistics."""
        self.cursor.execute("SELECT COUNT(*) FROM nodes")
        total_nodes = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM edges")
        total_edges = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT type, COUNT(*) FROM nodes GROUP BY type")
        type_counts = {row[0]: row[1] for row in self.cursor.fetchall()}
        
        source_count = type_counts.get("source", 0)
        sink_count = type_counts.get("export", 0)
        
        # Calculate average depth
        avg_depth = 0
        if total_nodes > 0:
            self.cursor.execute("SELECT COUNT(DISTINCT from_id) FROM edges")
            avg_depth = (total_edges / total_nodes) if total_nodes > 0 else 0
        
        return {
            "total_nodes": total_nodes,
            "total_edges": total_edges,
            "avg_depth": round(avg_depth, 2),
            "source_count": source_count,
            "sink_count": sink_count,
            "type_breakdown": type_counts
        }

    def close(self):
        """Close database connection."""
        self.conn.close()


def main():
    parser = argparse.ArgumentParser(description="Data Lineage Tracker")
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Register source
    reg_parser = subparsers.add_parser("register-source", help="Register a data source")
    reg_parser.add_argument("name", help="Source name")
    reg_parser.add_argument("type", help="Node type (source/sink/transform/etc)")
    reg_parser.add_argument("--connection", default="", help="Connection string")
    reg_parser.add_argument("--description", default="", help="Description")
    
    # Get lineage
    lineage_parser = subparsers.add_parser("lineage", help="Get lineage for a node")
    lineage_parser.add_argument("node_name", help="Node name")
    lineage_parser.add_argument("--direction", choices=["upstream", "downstream", "both"], default="both")
    
    # Impact analysis
    impact_parser = subparsers.add_parser("impact", help="Analyze impact of changes")
    impact_parser.add_argument("node_name", help="Node name")
    
    args = parser.parse_args()
    
    tracker = DataLineageTracker()
    
    try:
        if args.command == "register-source":
            node = tracker.register_source(args.name, args.type, args.connection, args.description)
            print(f"✓ Registered {args.type}: {args.name}")
        
        elif args.command == "lineage":
            viz = tracker.visualize_ascii(args.node_name)
            print(viz)
        
        elif args.command == "impact":
            impact = tracker.impact_analysis(args.node_name)
            print(f"Impact Analysis for: {impact['node']}")
            print(f"  Severity: {impact['severity']}")
            print(f"  Impacted nodes: {impact['impact_count']}")
            for dep in impact['directly_impacted'][:5]:
                print(f"    - {dep}")
    
    finally:
        tracker.close()


if __name__ == "__main__":
    main()
