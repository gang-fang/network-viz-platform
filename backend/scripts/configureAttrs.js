#!/usr/bin/env node
/**
 * configure:attrs — interactive helper to populate NODE_ATTRIBUTE_FILES in .env
 *
 * Scans the node-attribute directory, lets the user pick which .nodes.attr files
 * to load, then writes the choice back into .env.
 *
 * Usage: npm run configure:attrs
 */

'use strict';

const fs     = require('fs');
const path   = require('path');
const rl     = require('readline');
const dotenv = require('dotenv');

// ─── Paths ────────────────────────────────────────────────────────────────────

const ROOT    = path.join(__dirname, '../..');
const DOTENV  = path.join(ROOT, '.env');

// ─── .env helpers ─────────────────────────────────────────────────────────────

/** Read .env as a string, or '' if it doesn't exist. */
function readEnvFile() {
    try { return fs.readFileSync(DOTENV, 'utf8'); }
    catch { return ''; }
}

/** Extract a single value from an env string (returns '' if the key is absent or blank). */
function getEnvValue(content, key) {
    // dotenv.parse() handles quoted values and matches runtime dotenv behaviour,
    // avoiding divergence for entries like KEY="value" or KEY='value'.
    const parsed = dotenv.parse(content);
    return (parsed[key] || '').trim();
}

/** Return the currently configured file list (may be empty). */
function getCurrentFiles(content) {
    return getEnvValue(content, 'NODE_ATTRIBUTE_FILES')
        .split(',').map(f => f.trim()).filter(Boolean);
}

/**
 * Rewrite (or append) a single KEY=value line in an env string.
 * Preserves all other content exactly.
 */
function setEnvValue(content, key, value) {
    const line = `${key}=${value}`;
    // Use [ \t]* (horizontal whitespace only) to avoid crossing line boundaries.
    if (new RegExp(`^${key}[ \\t]*=`, 'm').test(content)) {
        return content.replace(new RegExp(`^${key}[ \\t]*=.*$`, 'm'), line);
    }
    // Key not present — append after a blank line
    return content.replace(/\n*$/, '\n') + line + '\n';
}

// ─── Attribute directory ──────────────────────────────────────────────────────

/** Resolve the attribute directory from .env (or fall back to the default). */
function resolveAttrDir(content) {
    const raw = getEnvValue(content, 'NODE_ATTRIBUTES_PATH');
    return raw ? path.resolve(ROOT, raw) : path.join(ROOT, 'data', 'nodes_attr');
}

// ─── Prompt helpers ───────────────────────────────────────────────────────────

/**
 * Build a readline interface whose ask() works correctly for both interactive
 * terminals and piped input.
 *
 * readline's built-in question() registers a callback for the *next* 'line'
 * event.  With piped stdin that arrives faster than sequential question() calls
 * can be registered, lines are consumed before the callback is set up and are
 * permanently lost.  Driving readline through a single persistent 'line'
 * listener with a resolver queue fixes this for both modes.
 */
function buildPrompter() {
    const lineQueue     = [];   // lines received before ask() was called
    const resolverQueue = [];   // ask() promises waiting for a line

    const iface = rl.createInterface({ input: process.stdin, output: process.stdout });

    iface.on('line', line => {
        if (resolverQueue.length > 0) {
            resolverQueue.shift()(line);
        } else {
            lineQueue.push(line);
        }
    });

    // Resolve any pending ask() with '' when stdin closes (EOF without answer).
    iface.on('close', () => {
        while (resolverQueue.length > 0) resolverQueue.shift()('');
    });

    return {
        ask(prompt) {
            process.stdout.write(prompt);
            return new Promise(resolve => {
                if (lineQueue.length > 0) {
                    resolve(lineQueue.shift());
                } else {
                    resolverQueue.push(resolve);
                }
            });
        },
        close() { iface.close(); },
    };
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
    const envContent = readEnvFile();
    const attrDir    = resolveAttrDir(envContent);
    const current    = getCurrentFiles(envContent);

    // ── 1. Scan for available files ───────────────────────────────────────────

    if (!fs.existsSync(attrDir)) {
        console.error(`\nAttribute directory not found: ${attrDir}`);
        console.error('Create the directory and place your .nodes.attr files there, then re-run.\n');
        process.exit(1);
    }

    const available = fs.readdirSync(attrDir)
        .filter(f => f.endsWith('.nodes.attr'))
        .sort();

    if (available.length === 0) {
        console.error(`\nNo .nodes.attr files found in: ${attrDir}`);
        console.error('Add your attribute files there and re-run.\n');
        process.exit(1);
    }

    // ── 2. Display the menu ───────────────────────────────────────────────────

    console.log(`\nAttribute files in: ${attrDir}\n`);
    available.forEach((f, i) => {
        const marker = current.includes(f) ? '[x]' : '[ ]';
        console.log(`  ${String(i + 1).padStart(2)}.  ${marker}  ${f}`);
    });

    const staleFiles = current.filter(f => !available.includes(f));
    if (staleFiles.length > 0) {
        console.log(`\n  Warning: these files are currently selected but not found on disk:`);
        staleFiles.forEach(f => console.log(`    - ${f}`));
    }

    if (current.length > 0) {
        console.log(`\nCurrently active: ${current.join(', ')}`);
    } else {
        console.log('\nNo files currently active (NODE_ATTRIBUTE_FILES is empty or unset).');
    }

    // ── 3. Prompt for selection ───────────────────────────────────────────────

    const prompter = buildPrompter();

    console.log('\nEnter the number(s) of the files to activate, separated by spaces or commas.');
    console.log('Leave blank to keep the current selection unchanged.\n');

    let chosen;
    while (true) {
        const raw = await prompter.ask('Selection: ');

        if (!raw.trim()) {
            // Blank = no change. Exit without touching .env — an empty
            // NODE_ATTRIBUTE_FILES would make the next `npm run ingest` fail.
            prompter.close();
            console.log('\nNo selection made — .env was not changed.\n');
            process.exit(0);
        }

        const parts  = raw.split(/[\s,]+/).filter(Boolean);
        const result = [];
        let   valid  = true;

        for (const part of parts) {
            const n = parseInt(part, 10);
            if (isNaN(n) || n < 1 || n > available.length) {
                console.log(`  Invalid number: "${part}" — enter values between 1 and ${available.length}.`);
                valid = false;
                break;
            }
            const file = available[n - 1];
            if (!result.includes(file)) result.push(file);
        }

        if (valid) { chosen = result; break; }
    }

    // ── 4. Confirm ────────────────────────────────────────────────────────────

    console.log(`\nNODE_ATTRIBUTE_FILES will be set to: ${chosen.join(',')}`);

    const answer = await prompter.ask('\nWrite to .env? [y/N] ');
    prompter.close();

    // Default is NO — require an explicit 'y' or 'yes' to proceed.
    // This also prevents a silent write when stdin closes before this prompt
    // (readline resolves with '' on EOF, which correctly falls through to abort).
    if (!['y', 'yes'].includes(answer.trim().toLowerCase())) {
        console.log('\nAborted — .env was not changed.\n');
        process.exit(0);
    }

    // ── 5. Write .env ─────────────────────────────────────────────────────────

    const updated = setEnvValue(envContent, 'NODE_ATTRIBUTE_FILES', chosen.join(','));
    fs.writeFileSync(DOTENV, updated, 'utf8');

    console.log(`\n.env updated — NODE_ATTRIBUTE_FILES=${chosen.join(',')}`);
    console.log('\nNext step:  npm run ingest\n');
}

main().catch(err => {
    console.error(`\nUnexpected error: ${err.message}\n`);
    process.exit(1);
});
