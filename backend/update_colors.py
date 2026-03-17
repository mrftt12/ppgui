import json
import sys

notebook_path = "/Users/admin/Desktop/solver_comparison_heatmap.ipynb"

new_code = """# Discrete coloring logic
bus_colors = []
for dev in voltage_deviation:
    if dev <= 0.01:
        bus_colors.append('#2ecc71') # Green
    elif dev <= 0.03:
        bus_colors.append('#f1c40f') # Yellow
    elif dev <= 0.05:
        bus_colors.append('#f39c12') # Orange
    else:
        bus_colors.append('#e74c3c') # Red

# Draw buses colored by voltage deviation
sc = plt.scatter(
    [pos[i][0] for i in range(1, n_buses+1)],
    [pos[i][1] for i in range(1, n_buses+1)],
    c=bus_colors,
    s=150,
    edgecolors='#fbfbff',
    linewidth=1, 
    zorder=2
)

# Custom Legend
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

legend_elements = [
    Line2D([0], [0], marker='o', color='w', label='<= 1%',
           markerfacecolor='#2ecc71', markersize=10),
    Line2D([0], [0], marker='o', color='w', label='1-3%',
           markerfacecolor='#f1c40f', markersize=10),
    Line2D([0], [0], marker='o', color='w', label='3-5%',
           markerfacecolor='#f39c12', markersize=10),
    Line2D([0], [0], marker='o', color='w', label='> 5%',
           markerfacecolor='#e74c3c', markersize=10)
]

# Add legend with dark background compatible styling if needed, assuming dark theme from previous context
plt.legend(handles=legend_elements, loc='upper right', title='Voltage Deviation |V_nr - V_gs|', fontsize=10)
"""

with open(notebook_path, "r", encoding="utf-8") as f:
    nb = json.load(f)

found = False
for cell in nb["cells"]:
    if cell["cell_type"] == "code":
        source_str = "".join(cell["source"])
        if "c=voltage_deviation" in source_str and "cmap='RdYlGn_r'" in source_str:
            lines = cell["source"]
            new_lines = []
            skip = False
            inserted = False

            for line in lines:
                if "sc = plt.scatter(" in line:
                    skip = True
                    # Insert new code here
                    if not inserted:
                        new_lines.append(new_code)
                        inserted = True
                elif "cbar.ax.yaxis.label.set_color" in line:
                    skip = False
                    continue

                if not skip:
                    if "cbar =" in line:
                        pass
                    else:
                        new_lines.append(line)

            cell["source"] = new_lines
            found = True
            print("Found and modified the visualization cell.")
            break

if not found:
    print("Could not find the target cell.")
    sys.exit(1)

with open(notebook_path, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1)

print("Notebook updated successfully.")
