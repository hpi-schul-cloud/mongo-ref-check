#!/usr/bin/env python3
"""
Visualize database collection relationships from relations.yaml

Options:
1. ASCII art (no dependencies) - simple text-based graph
2. Graphviz DOT output (requires graphviz to render)
3. NetworkX + matplotlib (requires: pip install networkx matplotlib)
4. Rich console tree (requires: pip install rich)
"""

import yaml
import argparse
from collections import defaultdict


def load_config(config_path="relations.yaml"):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def extract_relationships(config):
    """Extract all relationships from config into a list of (source, target, label) tuples."""
    relationships = []
    collections = set()
    
    for relation in config['relations']:
        source = relation['collection']
        collections.add(source)
        
        for field in relation['fields']:
            field_name = field['field']
            is_array = field.get('is_array', False)
            optional = field.get('optional', False)
            
            # Build label
            modifiers = []
            if is_array:
                modifiers.append("[]")
            if optional:
                modifiers.append("?")
            modifier_str = "".join(modifiers)
            
            if 'discriminator' in field:
                # Polymorphic reference
                disc = field['discriminator']
                for case in field['cases']:
                    target = case['references_collection']
                    collections.add(target)
                    label = f"{field_name}{modifier_str} ({disc}={case['value']})"
                    relationships.append((source, target, label))
            else:
                target = field['references_collection']
                collections.add(target)
                ref_field = field.get('references_field', '_id')
                if ref_field == '_id':
                    label = f"{field_name}{modifier_str}"
                else:
                    label = f"{field_name}{modifier_str} → {ref_field}"
                relationships.append((source, target, label))
    
    return relationships, collections


# =============================================================================
# Option 1: ASCII Art (no dependencies)
# =============================================================================

def generate_ascii(config):
    """Generate simple ASCII representation of relationships."""
    relationships, collections = extract_relationships(config)
    
    # Group by source collection
    by_source = defaultdict(list)
    for src, tgt, label in relationships:
        by_source[src].append((tgt, label))
    
    output = []
    output.append("=" * 70)
    output.append("  DATABASE COLLECTION RELATIONSHIPS")
    output.append("=" * 70)
    output.append("")
    
    for collection in sorted(by_source.keys()):
        refs = by_source[collection]
        output.append(f"┌{'─' * (len(collection) + 2)}┐")
        output.append(f"│ {collection} │")
        output.append(f"└{'─' * (len(collection) + 2)}┘")
        
        for i, (target, label) in enumerate(refs):
            is_last = (i == len(refs) - 1)
            prefix = "    └──" if is_last else "    ├──"
            output.append(f"{prefix} {label}")
            output.append(f"    {'   ' if is_last else '│  '} └──→ [{target}]")
        
        output.append("")
    
    return "\n".join(output)


def generate_ascii_matrix(config):
    """Generate ASCII adjacency matrix showing relationships."""
    relationships, collections = extract_relationships(config)
    
    # Create adjacency count matrix
    coll_list = sorted(collections)
    matrix = defaultdict(int)
    for src, tgt, _ in relationships:
        matrix[(src, tgt)] += 1
    
    # Determine column width
    max_name = max(len(c) for c in coll_list)
    col_width = 3
    
    output = []
    output.append("=" * 70)
    output.append("  ADJACENCY MATRIX (rows → columns)")
    output.append("=" * 70)
    output.append("")
    
    # Header row with abbreviated names
    abbrevs = {c: c[:col_width] for c in coll_list}
    header = " " * (max_name + 2) + " ".join(f"{abbrevs[c]:>{col_width}}" for c in coll_list)
    output.append(header)
    output.append("-" * len(header))
    
    for src in coll_list:
        row = f"{src:>{max_name}} │"
        for tgt in coll_list:
            count = matrix.get((src, tgt), 0)
            if count > 0:
                row += f" {count:>{col_width-1}}│"
            else:
                row += f" {'·':>{col_width-1}}│"
        output.append(row)
    
    output.append("")
    output.append("Legend: Number = count of relationships from row → column")
    output.append("        · = no relationship")
    
    return "\n".join(output)


def generate_ascii_graph(config):
    """Generate a more visual ASCII graph."""
    relationships, collections = extract_relationships(config)
    
    # Calculate in-degree and out-degree for each collection
    in_degree = defaultdict(int)
    out_degree = defaultdict(int)
    
    for src, tgt, _ in relationships:
        out_degree[src] += 1
        in_degree[tgt] += 1
    
    output = []
    output.append("=" * 70)
    output.append("  COLLECTION GRAPH OVERVIEW")
    output.append("=" * 70)
    output.append("")
    output.append("  Collection              In   Out   Visual")
    output.append("  " + "-" * 50)
    
    for coll in sorted(collections):
        in_d = in_degree.get(coll, 0)
        out_d = out_degree.get(coll, 0)
        
        # Visual representation
        in_arrows = "←" * min(in_d, 5) + ("+" if in_d > 5 else "")
        out_arrows = "→" * min(out_d, 5) + ("+" if out_d > 5 else "")
        
        visual = f"{in_arrows:>6} [{coll[:15]:^15}] {out_arrows:<6}"
        output.append(f"  {coll:<22} {in_d:>3}  {out_d:>3}   {visual}")
    
    output.append("")
    output.append("  ← = incoming reference, → = outgoing reference")
    output.append("  + = more than 5")
    
    return "\n".join(output)


# =============================================================================
# Option 2: Graphviz DOT output
# =============================================================================

def generate_dot(config):
    """Generate Graphviz DOT format for visualization."""
    relationships, collections = extract_relationships(config)
    
    lines = []
    lines.append("digraph Relations {")
    lines.append("    rankdir=LR;")
    lines.append("    node [shape=box, style=filled, fillcolor=lightblue];")
    lines.append("    edge [fontsize=9];")
    lines.append("")
    
    # Add all collections as nodes
    for coll in sorted(collections):
        safe_name = coll.replace("-", "_")
        lines.append(f'    {safe_name} [label="{coll}"];')
    
    lines.append("")
    
    # Add edges
    for src, tgt, label in relationships:
        safe_src = src.replace("-", "_")
        safe_tgt = tgt.replace("-", "_")
        # Escape quotes in label
        safe_label = label.replace('"', '\\"')
        lines.append(f'    {safe_src} -> {safe_tgt} [label="{safe_label}"];')
    
    lines.append("}")
    
    return "\n".join(lines)


# =============================================================================
# Option 3: NetworkX + Matplotlib
# =============================================================================

def generate_matplotlib(config, output_file="relations_graph.png"):
    """Generate a PNG image using NetworkX and Matplotlib."""
    try:
        import networkx as nx
        import matplotlib.pyplot as plt
    except ImportError:
        return "Error: Please install networkx and matplotlib:\n  pip install networkx matplotlib"
    
    relationships, collections = extract_relationships(config)
    
    # Create directed graph
    G = nx.DiGraph()
    
    # Add nodes
    for coll in collections:
        G.add_node(coll)
    
    # Add edges
    for src, tgt, label in relationships:
        if G.has_edge(src, tgt):
            # Append to existing label
            G[src][tgt]['label'] += f"\n{label}"
        else:
            G.add_edge(src, tgt, label=label)
    
    # Create figure
    plt.figure(figsize=(20, 16))
    
    # Use spring layout for positioning
    pos = nx.spring_layout(G, k=2, iterations=50, seed=42)
    
    # Draw nodes
    node_colors = []
    for node in G.nodes():
        out_deg = G.out_degree(node)
        in_deg = G.in_degree(node)
        if out_deg > in_deg:
            node_colors.append('#90EE90')  # Light green - more outgoing
        elif in_deg > out_deg:
            node_colors.append('#FFB6C1')  # Light pink - more incoming
        else:
            node_colors.append('#87CEEB')  # Light blue - balanced
    
    nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=2000, alpha=0.9)
    nx.draw_networkx_labels(G, pos, font_size=8, font_weight='bold')
    
    # Draw edges
    nx.draw_networkx_edges(G, pos, edge_color='gray', 
                           arrows=True, arrowsize=20, 
                           connectionstyle="arc3,rad=0.1",
                           alpha=0.7)
    
    plt.title("Database Collection Relationships\n(Green=more refs out, Pink=more refs in, Blue=balanced)", 
              fontsize=14)
    plt.axis('off')
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close()
    
    return f"Graph saved to: {output_file}"


# =============================================================================
# Option 4: Rich Console Tree
# =============================================================================

def generate_rich_tree(config):
    """Generate a rich console tree visualization."""
    try:
        from rich.console import Console
        from rich.tree import Tree
        from rich.panel import Panel
        from rich import print as rprint
    except ImportError:
        return "Error: Please install rich:\n  pip install rich"
    
    relationships, collections = extract_relationships(config)
    
    # Group by source
    by_source = defaultdict(list)
    for src, tgt, label in relationships:
        by_source[src].append((tgt, label))
    
    console = Console(record=True)
    
    root = Tree("📦 [bold blue]Database Relations[/bold blue]")
    
    for collection in sorted(by_source.keys()):
        refs = by_source[collection]
        coll_branch = root.add(f"🗃️  [bold green]{collection}[/bold green]")
        
        for target, label in refs:
            coll_branch.add(f"[yellow]{label}[/yellow] → [cyan]{target}[/cyan]")
    
    console.print(root)
    return "Rich tree printed to console"


# =============================================================================
# Option 5: Mermaid diagram (for markdown/web)
# =============================================================================

def generate_mermaid(config):
    """Generate Mermaid diagram syntax."""
    relationships, collections = extract_relationships(config)
    
    lines = []
    lines.append("```mermaid")
    lines.append("graph LR")
    lines.append("")
    
    # Group edges to avoid duplicates in visual
    edge_count = defaultdict(int)
    for src, tgt, label in relationships:
        edge_count[(src, tgt)] += 1
    
    for (src, tgt), count in edge_count.items():
        safe_src = src.replace("-", "_")
        safe_tgt = tgt.replace("-", "_")
        if count > 1:
            lines.append(f"    {safe_src}[{src}] -->|{count} refs| {safe_tgt}[{tgt}]")
        else:
            lines.append(f"    {safe_src}[{src}] --> {safe_tgt}[{tgt}]")
    
    lines.append("```")
    
    return "\n".join(lines)


# =============================================================================
# Statistics
# =============================================================================

def generate_stats(config):
    """Generate statistics about the relationships."""
    relationships, collections = extract_relationships(config)
    
    in_degree = defaultdict(int)
    out_degree = defaultdict(int)
    
    for src, tgt, _ in relationships:
        out_degree[src] += 1
        in_degree[tgt] += 1
    
    output = []
    output.append("=" * 70)
    output.append("  RELATIONSHIP STATISTICS")
    output.append("=" * 70)
    output.append(f"\n  Total collections: {len(collections)}")
    output.append(f"  Total relationships: {len(relationships)}")
    output.append("")
    
    output.append("  Top 10 collections by OUTGOING references (most dependent):")
    for coll, count in sorted(out_degree.items(), key=lambda x: -x[1])[:10]:
        output.append(f"    {coll}: {count}")
    
    output.append("")
    output.append("  Top 10 collections by INCOMING references (most depended upon):")
    for coll, count in sorted(in_degree.items(), key=lambda x: -x[1])[:10]:
        output.append(f"    {coll}: {count}")
    
    # Find isolated collections (defined but no refs)
    isolated = [c for c in collections if in_degree[c] == 0 and out_degree[c] == 0]
    if isolated:
        output.append("")
        output.append("  Isolated collections (no incoming or outgoing refs):")
        for c in isolated:
            output.append(f"    {c}")
    
    # Find root collections (no incoming refs, only outgoing)
    roots = [c for c in collections if in_degree[c] == 0 and out_degree[c] > 0]
    if roots:
        output.append("")
        output.append("  Root collections (no incoming refs):")
        for c in sorted(roots):
            output.append(f"    {c}")
    
    # Find leaf collections (no outgoing refs, only incoming)
    leaves = [c for c in collections if out_degree[c] == 0 and in_degree[c] > 0]
    if leaves:
        output.append("")
        output.append("  Leaf collections (no outgoing refs, only referenced by others):")
        for c in sorted(leaves):
            output.append(f"    {c}")
    
    return "\n".join(output)


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Visualize database collection relationships",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Output formats:
  ascii     - Simple ASCII tree (no dependencies)
  matrix    - ASCII adjacency matrix
  graph     - ASCII graph overview with in/out degree
  dot       - Graphviz DOT format (pipe to: dot -Tpng -o graph.png)
  png       - PNG image (requires: pip install networkx matplotlib)
  rich      - Rich console tree (requires: pip install rich)
  mermaid   - Mermaid diagram syntax (for markdown)
  stats     - Statistics about relationships
  all       - Show all ASCII outputs

Examples:
  python visualize_relations.py ascii
  python visualize_relations.py dot > relations.dot && dot -Tpng relations.dot -o relations.png
  python visualize_relations.py png --output my_graph.png
        """
    )
    
    parser.add_argument("format", 
                        choices=["ascii", "matrix", "graph", "dot", "png", "rich", "mermaid", "stats", "all"],
                        help="Output format")
    parser.add_argument("-c", "--config", default="relations.yaml",
                        help="Path to relations.yaml (default: relations.yaml)")
    parser.add_argument("-o", "--output", default="relations_graph.png",
                        help="Output file for PNG format (default: relations_graph.png)")
    
    args = parser.parse_args()
    
    config = load_config(args.config)
    
    if args.format == "ascii":
        print(generate_ascii(config))
    elif args.format == "matrix":
        print(generate_ascii_matrix(config))
    elif args.format == "graph":
        print(generate_ascii_graph(config))
    elif args.format == "dot":
        print(generate_dot(config))
    elif args.format == "png":
        result = generate_matplotlib(config, args.output)
        print(result)
    elif args.format == "rich":
        result = generate_rich_tree(config)
        if result.startswith("Error"):
            print(result)
    elif args.format == "mermaid":
        print(generate_mermaid(config))
    elif args.format == "stats":
        print(generate_stats(config))
    elif args.format == "all":
        print(generate_ascii(config))
        print("\n" + "=" * 70 + "\n")
        print(generate_ascii_graph(config))
        print("\n" + "=" * 70 + "\n")
        print(generate_stats(config))


if __name__ == "__main__":
    main()

