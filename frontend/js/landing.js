const DEFAULT_LIMITS = {
    allowedIndexes: ['ba', 'eu'],
    maxSeedCount: 10,
    maxInputLength: 2000,
    maxNameLength: 80,
    defaultMaxNodes: 500,
    maxNodesLimit: 2500,
};

let currentLimits = { ...DEFAULT_LIMITS };
let lastValidMaxNodesValue = String(DEFAULT_LIMITS.defaultMaxNodes);
let hasUserEditedMaxNodes = false;
let viewerReadyRequestId = 0;

function getRecommendedMaxNodesCopy(limit) {
    return `For smoother visualization, neighboring proteins are capped at ${limit.toLocaleString()}; 500–1,000 is recommended.`;
}

function formatMaxNodesLimit(limit) {
    return Number(limit).toLocaleString();
}

function getDomainLabel(indexName) {
    const normalized = String(indexName || '').trim().toLowerCase();
    if (normalized === 'ba') return 'Bacteria';
    if (normalized === 'eu') return 'Eukaryota';
    // Fall back to the backend-provided code for any future domain additions.
    return indexName;
}

function playWarningSound() {
    const AudioContextCtor = window.AudioContext || window.webkitAudioContext;
    if (!AudioContextCtor) return;

    if (!playWarningSound.audioContext) {
        playWarningSound.audioContext = new AudioContextCtor();
    }

    const audioContext = playWarningSound.audioContext;
    if (audioContext.state === 'suspended') {
        audioContext.resume().catch(() => {});
    }

    const oscillator = audioContext.createOscillator();
    const gainNode = audioContext.createGain();
    const startTime = audioContext.currentTime;

    oscillator.type = 'triangle';
    oscillator.frequency.setValueAtTime(880, startTime);
    gainNode.gain.setValueAtTime(0.0001, startTime);
    gainNode.gain.exponentialRampToValueAtTime(0.06, startTime + 0.01);
    gainNode.gain.exponentialRampToValueAtTime(0.0001, startTime + 0.12);

    oscillator.connect(gainNode);
    gainNode.connect(audioContext.destination);
    oscillator.start(startTime);
    oscillator.stop(startTime + 0.12);
}

function parseSeeds(text) {
    return text
        .split(/[\s,;]+/)
        .map(token => token.trim())
        .filter(Boolean);
}

function buildViewerUrlWithSeeds(viewerUrl, seeds) {
    if (!viewerUrl || !seeds.length) {
        return viewerUrl;
    }

    const url = new URL(viewerUrl, window.location.origin);
    url.searchParams.set('seeds', seeds.join(','));
    return `${url.pathname}${url.search}`;
}

async function waitForViewerReady({ network, expectedEdgeCount, requestId, viewerStatusUrl }) {
    const statusUrl = viewerStatusUrl || `/api/networks/${encodeURIComponent(network)}/status`;
    const maxAttempts = 30;
    const retryDelayMs = 500;

    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
        if (requestId !== viewerReadyRequestId) {
            return false;
        }

        try {
            const response = await fetch(statusUrl, { cache: 'no-store' });
            if (response.ok) {
                const status = await response.json();
                if (status.ready && status.edgeCount === expectedEdgeCount && status.nodeCount > 0) {
                    return true;
                }
            }
        } catch (_err) {
            // Retry until timeout.
        }

        await new Promise(resolve => window.setTimeout(resolve, retryDelayMs));
    }

    return false;
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

function setMaxNodesInlineError(message = '') {
    const maxNodesInput = document.getElementById('max-nodes');
    const maxNodesError = document.getElementById('max-nodes-error');
    if (!maxNodesInput || !maxNodesError) return;

    const hasError = Boolean(message);
    maxNodesInput.classList.toggle('input-invalid', hasError);
    maxNodesInput.setCustomValidity(message);
    maxNodesError.textContent = message;
    maxNodesError.classList.toggle('hidden', !hasError);
}

function setWorkingState(message) {
    renderResult({
        title: 'Working',
        summary: message,
        detailsHtml: '',
    });
}

function validateMaxNodesValue(maxNodes) {
    const parsedMaxNodes = Number(maxNodes);
    if (!Number.isInteger(parsedMaxNodes) || parsedMaxNodes < 1 || parsedMaxNodes > currentLimits.maxNodesLimit) {
        return `Max neighbors must be an integer between 1 and ${formatMaxNodesLimit(currentLimits.maxNodesLimit)}.`;
    }
    return null;
}

function updateLimitsUi(limits) {
    currentLimits = { ...DEFAULT_LIMITS, ...limits };

    const nameInput = document.getElementById('subnetwork-name');
    const maxNodesInput = document.getElementById('max-nodes');
    const indexInput = document.getElementById('graph-index');
    const seedHelp = document.getElementById('seed-help');
    const nameHelp = document.getElementById('name-help');
    const maxNodesHelp = document.getElementById('max-nodes-help');

    nameInput.maxLength = currentLimits.maxNameLength;
    maxNodesInput.min = '1';
    maxNodesInput.max = String(currentLimits.maxNodesLimit);
    if (!hasUserEditedMaxNodes) {
        maxNodesInput.value = String(currentLimits.defaultMaxNodes);
        lastValidMaxNodesValue = maxNodesInput.value;
    }

    nameHelp.innerHTML =
        `Letters, numbers, <code>_</code>, <code>-</code>, and <code>.</code> only. ` +
        `<code>.csv</code> is added automatically. Maximum ${currentLimits.maxNameLength} characters.`;
    seedHelp.textContent =
        `Separate UniProt ACs with new lines, spaces, commas, or semicolons. ` +
        `Maximum ${currentLimits.maxSeedCount} ACs.`;
    maxNodesHelp.textContent = getRecommendedMaxNodesCopy(currentLimits.maxNodesLimit);
    setMaxNodesInlineError('');

    indexInput.innerHTML = '';
    currentLimits.allowedIndexes.forEach(indexName => {
        const option = document.createElement('option');
        option.value = indexName;
        option.textContent = getDomainLabel(indexName);
        indexInput.appendChild(option);
    });
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
        return { field: 'name', message: 'Subnetwork name is required.' };
    }

    if (name.length > currentLimits.maxNameLength) {
        return { field: 'name', message: `Subnetwork name must be ${currentLimits.maxNameLength} characters or fewer.` };
    }

    if (seeds.length === 0) {
        return { field: 'seeds', message: 'At least one seed protein is required.' };
    }

    if (seeds.length > currentLimits.maxSeedCount) {
        return { field: 'seeds', message: `Seed input is limited to ${currentLimits.maxSeedCount} identifiers.` };
    }

    const combinedLength = seeds.join('\n').length;
    if (combinedLength > currentLimits.maxInputLength) {
        return { field: 'seeds', message: `Seed input is limited to ${currentLimits.maxInputLength} characters.` };
    }

    const maxNodesError = validateMaxNodesValue(maxNodes);
    if (maxNodesError) {
        return { field: 'maxNodes', message: maxNodesError };
    }

    return null;
}

function enforceMaxNodesLimit() {
    const maxNodesInput = document.getElementById('max-nodes');
    const rawValue = maxNodesInput.value.trim();

    if (!rawValue) {
        setMaxNodesInlineError('');
        return;
    }

    const parsedValue = Number(rawValue);
    if (!Number.isFinite(parsedValue)) {
        setMaxNodesInlineError('Max neighbors must be a number.');
        return;
    }

    if (Number.isInteger(parsedValue) && parsedValue >= 1 && parsedValue <= currentLimits.maxNodesLimit) {
        lastValidMaxNodesValue = rawValue;
        setMaxNodesInlineError('');
        return;
    }

    if (parsedValue > currentLimits.maxNodesLimit) {
        maxNodesInput.value = lastValidMaxNodesValue;
        playWarningSound();
        setMaxNodesInlineError(`Max neighbors cannot exceed ${formatMaxNodesLimit(currentLimits.maxNodesLimit)}.`);
        return;
    }

    if (parsedValue < 1) {
        setMaxNodesInlineError('Max neighbors must be at least 1.');
        return;
    }

    if (!Number.isInteger(parsedValue)) {
        setMaxNodesInlineError('Max neighbors must be a whole number.');
    }
}

async function handleSubmit(event) {
    event.preventDefault();

    const seedInput = document.getElementById('seed-input');
    const nameInput = document.getElementById('subnetwork-name');
    const indexInput = document.getElementById('graph-index');
    const maxNodesInput = document.getElementById('max-nodes');
    const submitBtn = document.getElementById('submit-btn');

    const seeds = parseSeeds(seedInput.value);
    const requestId = ++viewerReadyRequestId;
    const payload = {
        seeds,
        name: nameInput.value.trim(),
        index: indexInput.value,
        maxNodes: maxNodesInput.value,
    };

    const validationError = validateBeforeSubmit(payload);
    if (validationError) {
        if (validationError.field === 'maxNodes' && !maxNodesInput.validationMessage) {
            setMaxNodesInlineError(validationError.message);
        }
        renderResult({
            title: 'Validation error',
            summary: validationError.message,
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

        const viewerUrl = buildViewerUrlWithSeeds(result.viewerUrl, seeds);
        const summary = `${result.network} created with ${result.edgeCount} edges from ${result.inputSeedCount || 0} submitted seeds.`;

        if (result.viewerReady === false) {
            renderResult({
                title: 'Finalizing network',
                summary: `${summary} Preparing it for the viewer...`,
                detailsHtml,
            });

            const isReady = await waitForViewerReady({
                network: result.network,
                expectedEdgeCount: result.edgeCount,
                requestId,
                viewerStatusUrl: result.viewerStatusUrl,
            });

            if (requestId !== viewerReadyRequestId) {
                return;
            }

            renderResult({
                title: isReady ? 'Subnetwork ready' : 'Viewer still finalizing',
                summary: isReady
                    ? summary
                    : `${summary} The network file was generated, but the viewer is still preparing it. Please try opening it again from the network list in a moment.`,
                detailsHtml,
                viewerUrl: isReady ? viewerUrl : undefined,
                isError: !isReady,
            });
            return;
        }

        renderResult({
            title: 'Subnetwork ready',
            summary,
            detailsHtml,
            viewerUrl,
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
    const maxNodesInput = document.getElementById('max-nodes');

    updateLimitsUi(DEFAULT_LIMITS);
    form.addEventListener('submit', handleSubmit);
    seedInput.addEventListener('input', updateSeedMeta);
    maxNodesInput.addEventListener('input', () => {
        hasUserEditedMaxNodes = true;
        enforceMaxNodesLimit();
    });
    maxNodesInput.addEventListener('change', enforceMaxNodesLimit);
    updateSeedMeta();
    await loadLimits();
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initLandingPage);
} else {
    initLandingPage();
}
