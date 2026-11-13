import json
from pathlib import Path
import subprocess


def test_profile_switch_survives_reload_and_preserves_creation_request_context():
    module = (
        Path(__file__).resolve().parents[1] / "bdw/bit_data_workbench/static/js/demo-identity.js"
    )
    script = """
import assert from 'node:assert/strict';
const storage = new Map();
globalThis.CustomEvent = class { constructor(type, options) { this.type=type; this.detail=options.detail; } };
globalThis.document = {documentElement:{dataset:{}}, body:{dataset:{}}, querySelector:()=>null, querySelectorAll:()=>[]};
globalThis.window = {
  location:{href:'http://local/notebooks/mwst?demoUser=jan.waser&creationRequest=request-id#cell'},
  localStorage:{getItem:key=>storage.get(key), setItem:(key,value)=>storage.set(key,value)},
  dispatchEvent:()=>{},
  history:{state:{notebookId:'mwst'}, replaceState(state,title,value){window.location.href=String(value);}},
};
globalThis.fetch=async()=>({});
const identity = await import(MODULE_URL);
assert.equal(identity.initializeDaaifDemoIdentity().id,'jan.waser');
identity.selectDaaifDemoUser('kassandra.valdata');
const url=new URL(window.location.href);
assert.equal(url.searchParams.get('demoUser'),'kassandra.valdata');
assert.equal(url.searchParams.get('creationRequest'),'request-id');
assert.equal(url.hash,'#cell');
assert.equal(identity.initializeDaaifDemoIdentity().id,'kassandra.valdata');
assert.equal(identity.currentDaaifDemoUser().organizationShort,'ESTV');
identity.selectDaaifDemoUser('unknown-user');
assert.equal(identity.currentDaaifDemoUser().id,'kassandra.valdata');
""".replace("MODULE_URL", json.dumps(module.as_uri()))
    completed = subprocess.run(
        ["node", "--input-type=module", "--eval", script], capture_output=True, text=True
    )
    assert completed.returncode == 0, completed.stderr
