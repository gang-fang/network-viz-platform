const DEFAULT_LIMITS = {
    allowedIndexes: ['eu'],
    maxSeedCount: 10,
    maxInputLength: 2000,
    maxNameLength: 80,
    defaultMaxNodes: 500,
    maxNodesLimit: 2000,
};

let currentLimits = { ...DEFAULT_LIMITS };

function parseSeeds(text) {
    return text
        .split(/[\s,;]+/)
        .map(token => token.trim())
        .filter(Boolean);
}

function escapeHtml(value) {
    return String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
}

function createList(title, items) {
    if (!items.length) return '';
    const escapedItems = items.map(item => `<li>${escapeHtml(item)}</li>`).join('');
    return `
        <section class="detail-block">
            <h3>${escapeHtml(title)}</h3>
            <ul>${escapedItems}</ul>
        </section>
    `;
}

function renderResult({ title, summary, detailsHtml, viewerUrl, isError = false }) {
    const resultPanel = document.getElementById('result-panel');
    const resultTitle = document.getElementById('result-title');
    const resultSummary = document.getElementById('result-summary');
    const resultDetails = document.getElementById('result-details');
    const viewerLink = document.getElementById('viewer-link');

    resultPanel.classList.remove('hidden');
    resultPanel.classList.toggle('error', isError);
    resultTitle.textContent = title;
    resultSummary.textContent = summary;
    resultDetails.innerHTML = detailsHtml || '';

    if (viewerUrl) {
        viewerLink.href = viewerUrl;
        viewerLink.classList.remove('hidden');
    } else {
        viewerLink.classList.add('hidden');
        viewerLink.removeAttribute('href');
    }
}

function setWorkingState(message) {
    renderResult({
        title: 'Working',
        summary: message,
        detailsHtml: '',
    });
}

function updateLimitsUi(limits) {
    currentLimits = { ...DEFAULT_LIMITS, ...limits };

    const nameInput = document.getElementById('subnetwork-name');
    const seedInput = document.getElementById('seed-input');
    const maxNodesInput = document.getElementById('max-nodes');
    const indexField = document.getElementById('graph-index-field');
    const indexInput = document.getElementById('graph-index');
    const seedHelp = document.getElementById('seed-help');
    const nameHelp = document.getElementById('name-help');
    const maxNodesHelp = document.getElementById('max-nodes-help');

    nameInput.maxLength = currentLimits.maxNameLength;
    maxNodesInput.min = '1';
    maxNodesInput.max = String(currentLimits.maxNodesLimit);
    maxNodesInput.value = String(currentLimits.defaultMaxNodes);

    nameHelp.innerHTML =
        `Letters, numbers, <code>_</code>, <code>-</code>, and <code>.</code> only. ` +
        `<code>.csv</code> is added automatically. Maximum ${currentLimits.maxNameLength} characters.`;
    seedHelp.textContent =
        `Separate identifiers with new lines, spaces, commas, or semicolons. ` +
        `Maximum ${currentLimits.maxSeedCount} identifiers.`;
    maxNodesHelp.innerHTML =
        `Used as <code>--max_nodes</code> when the Python process runs. ` +
        `Allowed range: 1-${currentLimits.maxNodesLimit}.`;

    indexInput.innerHTML = '';
    currentLimits.allowedIndexes.forEach(indexName => {
        const option = document.createElement('option');
        option.value = indexName;
        option.textContent = indexName;
        indexInput.appendChild(option);
    });

    indexField.classList.toggle('hidden', currentLimits.allowedIndexes.length <= 1);
}

async function loadLimits() {
    try {
        const response = await fetch('/api/subnetworks/limits');
        if (!response.ok) throw new Error(`Failed to load limits: ${response.status}`);
        const limits = await response.json();
        updateLimitsUi(limits);
    } catch (err) {
        console.warn(err);
        updateLimitsUi(DEFAULT_LIMITS);
    }

    updateSeedMeta();
}

function validateBeforeSubmit({ seeds, name, maxNodes }) {
    if (!name) {
        return 'Subnetwork name is required.';
    }

    if (name.length > currentLimits.maxNameLength) {
        return `Subnetwork name must be ${currentLimits.maxNameLength} characters or fewer.`;
    }

    if (seeds.length === 0) {
        return 'At least one seed protein is required.';
    }

    if (seeds.length > currentLimits.maxSeedCount) {
        return `Seed input is limited to ${currentLimits.maxSeedCount} identifiers.`;
    }

    const combinedLength = seeds.join('\n').length;
    if (combinedLength > currentLimits.maxInputLength) {
        return `Seed input is limited to ${currentLimits.maxInputLength} characters.`;
    }

    const parsedMaxNodes = Number(maxNodes);
    if (!Number.isInteger(parsedMaxNodes) || parsedMaxNodes < 1 || parsedMaxNodes > currentLimits.maxNodesLimit) {
        return `Max nodes must be an integer between 1 and ${currentLimits.maxNodesLimit}.`;
    }

    return null;
}

async function handleSubmit(event) {
    event.preventDefault();

    const seedInput = document.getElementById('seed-input');
    const nameInput = document.getElementById('subnetwork-name');
    const indexInput = document.getElementById('graph-index');
    const maxNodesInput = document.getElementById('max-nodes');
    const submitBtn = document.getElementById('submit-btn');

    const seeds = parseSeeds(seedInput.value);
    const payload = {
        seeds,
        name: nameInput.value.trim(),
        index: indexInput.value,
        maxNodes: maxNodesInput.value,
    };

    const validationError = validateBeforeSubmit(payload);
    if (validationError) {
        renderResult({
            title: 'Validation error',
            summary: validationError,
            detailsHtml: '',
            isError: true,
        });
        return;
    }

    submitBtn.disabled = true;
    submitBtn.textContent = 'Extracting...';
    setWorkingState('Running the extraction job...');

    try {
        const response = await fetch('/api/subnetworks', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(payload),
        });

        const result = await response.json();
        const warnings = result.warnings || [];
        const stdoutLines = result.stdoutLines || [];
        const detailsHtml = [
            createList('Warnings', warnings),
            createList('Missing identifiers', result.missingSeeds || []),
            createList('Process output', stdoutLines),
        ].join('');

        if (!response.ok) {
            renderResult({
                title: 'Extraction failed',
                summary: result.error || 'The subnetwork could not be generated.',
                detailsHtml,
                isError: true,
            });
            return;
        }

        renderResult({
            title: 'Subnetwork ready',
            summary: `${result.network} created with ${result.edgeCount} edges from ${result.inputSeedCount || 0} submitted seeds.`,
            detailsHtml,
            viewerUrl: result.viewerUrl,
        });
    } catch (err) {
        renderResult({
            title: 'Extraction failed',
            summary: err.message,
            detailsHtml: '',
            isError: true,
        });
    } finally {
        submitBtn.disabled = false;
        submitBtn.textContent = 'Extract Subnetwork';
    }
}

function updateSeedMeta() {
    const seedInput = document.getElementById('seed-input');
    const seedMeta = document.getElementById('seed-meta');
    const seeds = parseSeeds(seedInput.value);
    const label = seeds.length === 1 ? 'identifier' : 'identifiers';
    seedMeta.textContent = `${seeds.length}/${currentLimits.maxSeedCount} ${label} parsed`;
}

async function initLandingPage() {
    const form = document.getElementById('subnetwork-form');
    const seedInput = document.getElementById('seed-input');

    form.addEventListener('submit', handleSubmit);
    seedInput.addEventListener('input', updateSeedMeta);
    updateSeedMeta();
    await loadLimits();
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initLandingPage);
} else {
    initLandingPage();
}
