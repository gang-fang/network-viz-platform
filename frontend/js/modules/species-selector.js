/**
 * Species Selector Module
 * Allows filtering and highlighting nodes based on NCBI_txID (Species).
 */
const SpeciesSelectorModule = {
    id: "species-selector",
    speciesMap: new Map(), // NCBI_txID -> Species Name
    selectedSpecies: new Set(), // Set of selected NCBI_txIDs
    context: null,

    // UI Elements
    popup: null,
    checkboxContainer: null,

    selectedColor: "#e74c3c", // Default color

    init(context) {
        this.context = context;
        // 1. Fetch Species Data
        this.fetchSpeciesData();

        // 2. Create UI Button
        const btn = document.createElement('button');
        btn.textContent = "Highlight by Species";
        btn.className = "control-button";
        btn.style.width = "100%";
        btn.style.marginTop = "10px";
        btn.onclick = () => this.togglePopup();

        context.addPanelControl(btn);

        // 3. Create Popup (Hidden)
        this.createPopup();
    },

    async fetchSpeciesData() {
        try {
            const res = await fetch('/api/species-names');
            if (!res.ok) throw new Error("Failed to fetch species names");
            const data = await res.json();

            // Data is array of { ncbi_txid, species_name }
            data.forEach(item => {
                this.speciesMap.set(String(item.ncbi_txid), item.species_name);
            });
            this.populateCheckboxes();

        } catch (err) {
            console.error("Species Selector Error:", err);
        }
    },

    createPopup() {
        // Create a floating panel
        this.popup = document.createElement('div');
        this.popup.className = "floating-panel hidden";
        this.popup.style.width = "350px"; // Slightly wider for color circles
        this.popup.style.maxHeight = "450px";
        this.popup.style.display = "none"; // Start hidden

        // Header
        const header = document.createElement('div');
        header.className = "panel-header";
        header.innerHTML = `<h3>Select Species</h3><button class="close-button">×</button>`;
        header.querySelector('.close-button').onclick = () => this.togglePopup();

        // Content
        const content = document.createElement('div');
        content.className = "panel-content";

        // Controls (Select All / Deselect All)
        const controls = document.createElement('div');
        controls.style.marginBottom = "10px";
        controls.style.display = "flex";
        controls.style.justifyContent = "space-between"; // Spaced out

        const selectAllBtn = document.createElement('button');
        selectAllBtn.textContent = "Select All";
        selectAllBtn.className = "control-button";
        selectAllBtn.onclick = () => this.toggleAll(true);

        const deselectAllBtn = document.createElement('button');
        deselectAllBtn.textContent = "Deselect All";
        deselectAllBtn.className = "control-button";
        deselectAllBtn.onclick = () => this.toggleAll(false);

        controls.appendChild(selectAllBtn);
        controls.appendChild(deselectAllBtn);
        content.appendChild(controls);

        // Checkbox List
        this.checkboxContainer = document.createElement('div');
        this.checkboxContainer.className = "checkbox-group";
        content.appendChild(this.checkboxContainer);

        // Actions Footer
        const footer = document.createElement('div');
        footer.className = "panel-footer";
        footer.style.flexDirection = "row"; // Horizontal layout
        footer.style.gap = "10px";
        footer.style.alignItems = "center";
        footer.style.justifyContent = "space-between";

        // Color Picker (Circles)
        const colorContainer = document.createElement('div');
        colorContainer.style.display = "flex";
        colorContainer.style.gap = "5px";
        colorContainer.style.flexWrap = "wrap";

        const colors = [
            "#e74c3c", // Red
            "#e91e63", // Pink (Changed from Blue #3498db to avoid conflict with default node color)
            "#2ecc71", // Green
            "#f1c40f", // Yellow
            "#9b59b6", // Purple
            "#e67e22", // Orange
            "#1abc9c", // Teal
            "#34495e"  // Dark Blue/Grey
        ];

        colors.forEach(color => {
            const circle = document.createElement('div');
            circle.style.width = "20px";
            circle.style.height = "20px";
            circle.style.borderRadius = "50%";
            circle.style.backgroundColor = color;
            circle.style.cursor = "pointer";
            circle.style.border = this.selectedColor === color ? "2px solid #000" : "1px solid #ccc";

            circle.onclick = () => {
                this.selectedColor = color;
                // Update selection visual
                Array.from(colorContainer.children).forEach(c => c.style.border = "1px solid #ccc");
                circle.style.border = "2px solid #000";
            };

            colorContainer.appendChild(circle);
        });

        footer.appendChild(colorContainer);

        // Highlight Button (Right side)
        const highlightBtn = document.createElement('button');
        highlightBtn.textContent = "Highlight";
        highlightBtn.className = "control-button";
        highlightBtn.style.flex = "0 0 auto"; // Don't shrink
        highlightBtn.onclick = () => this.applyAction('highlight');

        footer.appendChild(highlightBtn);

        this.popup.appendChild(header);
        this.popup.appendChild(content);
        this.popup.appendChild(footer);

        document.body.appendChild(this.popup);

        // Make draggable
        this.makeDraggable(this.popup);
    },

    populateCheckboxes() {
        this.checkboxContainer.innerHTML = "";

        // Sort species by name
        const sortedSpecies = Array.from(this.speciesMap.entries())
            .sort((a, b) => a[1].localeCompare(b[1]));

        sortedSpecies.forEach(([txid, name]) => {
            const div = document.createElement('div');
            div.className = "checkbox-item";

            const checkbox = document.createElement('input');
            checkbox.type = "checkbox";
            checkbox.value = txid;
            checkbox.id = `species-${txid}`;
            checkbox.onchange = (e) => {
                if (e.target.checked) this.selectedSpecies.add(txid);
                else this.selectedSpecies.delete(txid);
            };

            const label = document.createElement('label');
            label.htmlFor = `species-${txid}`;
            label.textContent = name;
            label.style.fontSize = "0.9rem";
            label.style.color = "black"; // Explicitly black
            label.style.cursor = "pointer";

            div.appendChild(checkbox);
            div.appendChild(label);
            this.checkboxContainer.appendChild(div);
        });
    },

    togglePopup() {
        if (this.popup.style.display === "none") {
            this.popup.style.display = "block";
            // Center it
            this.popup.style.top = "50%";
            this.popup.style.left = "50%";
            this.popup.style.transform = "translate(-50%, -50%)";
        } else {
            this.popup.style.display = "none";
        }
    },

    toggleAll(select) {
        const checkboxes = this.checkboxContainer.querySelectorAll('input[type="checkbox"]');
        checkboxes.forEach(cb => {
            cb.checked = select;
            if (select) this.selectedSpecies.add(cb.value);
            else this.selectedSpecies.delete(cb.value);
        });
    },

    async fetchNodesForSpecies(networkName, speciesIds) {
        const res = await fetch('/api/networks/search-species', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                network: networkName,
                speciesIds: Array.from(speciesIds)
            })
        });
        if (!res.ok) throw new Error("Failed to fetch nodes for species");
        return await res.json();
    },

    async applyAction(action) {
        if (this.selectedSpecies.size === 0) {
            alert("Please select at least one species.");
            return;
        }

        const networkName = this.context.getCurrentNetwork();

        if (!networkName) {
            alert("No network selected.");
            return;
        }

        try {
            const result = await this.fetchNodesForSpecies(networkName, this.selectedSpecies);

            if (action === 'highlight') {
                const color = this.selectedColor;
                if (result.matches && result.matches.length > 0) {
                    const layerId = `species:${Date.now()}`;
                    this.context.addHighlightLayer(layerId, result.matches, color);
                } else {
                    alert("No nodes found for the selected species.");
                }
            }
        } catch (err) {
            console.error("Species action failed:", err);
            alert("Failed to apply action: " + err.message);
        }
    },

    makeDraggable(element) {
        const header = element.querySelector('.panel-header');
        let isDragging = false;
        let startX, startY, initialLeft, initialTop;

        header.onmousedown = (e) => {
            isDragging = true;
            startX = e.clientX;
            startY = e.clientY;
            const rect = element.getBoundingClientRect();
            initialLeft = rect.left;
            initialTop = rect.top;
            element.style.transform = "none"; // Remove translate to use absolute left/top
            element.style.left = initialLeft + "px";
            element.style.top = initialTop + "px";
            element.classList.add('dragging');
        };

        document.onmousemove = (e) => {
            if (!isDragging) return;
            const dx = e.clientX - startX;
            const dy = e.clientY - startY;
            element.style.left = (initialLeft + dx) + "px";
            element.style.top = (initialTop + dy) + "px";
        };

        document.onmouseup = (e) => {
            isDragging = false;
            element.classList.remove('dragging');
        };
    }
};

// Export
if (typeof module !== 'undefined' && module.exports) {
    module.exports = SpeciesSelectorModule;
} else {
    window.SpeciesSelectorModule = SpeciesSelectorModule;
}
