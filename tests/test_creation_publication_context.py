import json
from pathlib import Path
import subprocess


def test_general_request_publication_has_period_and_frequency_without_mwst_defaults():
    module = (
        Path(__file__).resolve().parents[1]
        / "bdw/bit_data_workbench/static/js/result-product-publication.js"
    )
    script = """
import assert from 'node:assert/strict';
globalThis.window={location:{href:'http://local/?demoUser=kassandra.valdata&creationRequest=11111111-1111-4111-8111-111111111111'},localStorage:{getItem:()=> 'kassandra.valdata'}};
globalThis.document={documentElement:{dataset:{}},body:{dataset:{}},querySelector:()=>null,querySelectorAll:()=>[]};
let context={id:'11111111-1111-4111-8111-111111111111',ownerUserId:'kassandra.valdata',status:'in_progress',requesterName:'Jan Waser',proposedTitle:'Finanzplanung',purpose:'Historische Auswertung für die Finanzplanung',basisFrom:'2016-01-01',basisUntil:'2025-12-31',updateFrequency:'daily'};
globalThis.fetch=async()=>({ok:true,json:async()=>context});
const {publishStoredResult}=await import(MODULE_URL);
const controller={openPublishDialog:async value=>value};
const job={notebookTitle:'Eigene Auswertung'};
const target={bucket:'saved-results',key:'analysis.parquet'};
const published=await publishStoredResult({job,target,controller});
assert.equal(published.metadataDefaults.title,'Finanzplanung');
assert.equal(published.metadataDefaults.updateFrequency,'daily');
assert.equal(published.metadataDefaults.customProperties.basisFrom,'2016-01-01');
assert.equal(published.metadataDefaults.customProperties.basisUntil,'2025-12-31');
assert.equal(published.metadataDefaults.customProperties.creationRequestId,context.id);
assert.equal(published.metadataDefaults.customProperties.cantonCode,undefined);
assert.equal(published.metadataDefaults.customProperties.taxYear,undefined);
assert.equal(published.metadataDefaults.domain,undefined);
context={...context,basisWindowType:'rolling',basisFrom:null,basisUntil:null,rollingWindowSize:12,rollingWindowUnit:'months'};
const rolling=await publishStoredResult({job,target:{bucket:'mwst-journey',key:'products/mwst_firmen_sg_2026.parquet'},controller});
assert.equal(rolling.metadataDefaults.customProperties.basisWindowType,'rolling');
assert.equal(rolling.metadataDefaults.customProperties.rollingWindowSize,'12');
assert.equal(rolling.metadataDefaults.customProperties.rollingWindowUnit,'months');
assert.equal(rolling.metadataDefaults.customProperties.basisFrom,undefined);
assert.equal(rolling.metadataDefaults.customProperties.cantonCode,undefined);
assert.equal(rolling.metadataDefaults.customProperties.taxYear,undefined);
context={...context,basisWindowType:'fixed',rollingWindowSize:null,rollingWindowUnit:null};
context={...context,basisFrom:null,basisUntil:null,updateFrequency:null,cantonCode:'SG',taxYear:2026};
const legacy=await publishStoredResult({job,target,controller});
assert.equal(legacy.metadataDefaults.customProperties.cantonCode,'SG');
assert.equal(legacy.metadataDefaults.customProperties.taxYear,'2026');
assert.equal(legacy.metadataDefaults.updateFrequency,'annually');
context={...context,ownerUserId:'other.owner'};
await assert.rejects(()=>publishStoredResult({job,target,controller}),/zugewiesene Data Owner/);
""".replace("MODULE_URL", json.dumps(module.as_uri()))
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", script], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def test_request_context_survives_workspace_to_notebook_navigation():
    module = (
        Path(__file__).resolve().parents[1]
        / "bdw/bit_data_workbench/static/js/notebook-url-helpers.js"
    )
    script = """
import assert from 'node:assert/strict';
globalThis.window={location:new URL('http://local/?creationRequest=request-id&demoUser=kassandra.valdata'),history:{pushState(state,title,url){window.location=new URL(url,window.location);}}};
const {createNotebookUrlHelpers}=await import(MODULE_URL);
const urls=createNotebookUrlHelpers({isLocalNotebookId:()=>false});
urls.pushQueryWorkbenchHistory();
assert.equal(window.location.searchParams.get('creationRequest'),'request-id');
urls.pushQueryWorkbenchDataSourcesHistory('source-1',{browse:true});
assert.equal(window.location.searchParams.get('source_id'),'source-1');
assert.equal(window.location.searchParams.get('browse'),'1');
assert.equal(window.location.searchParams.get('creationRequest'),'request-id');
urls.pushQueryWorkbenchDataSourceExplorerHistory('source-1');
assert.equal(window.location.searchParams.get('creationRequest'),'request-id');
assert.equal(window.location.searchParams.get('source_id'),'source-1');
urls.pushNotebookHistory('chosen-notebook');
assert.equal(window.location.pathname,'/notebooks/chosen-notebook');
assert.equal(window.location.searchParams.get('creationRequest'),'request-id');
assert.equal(window.location.searchParams.get('demoUser'),'kassandra.valdata');
urls.pushQueryRunsHistory();
assert.equal(window.location.searchParams.get('creationRequest'),'request-id');
window.location=new URL('http://local/');
assert.equal(urls.notebookUrl('ordinary-notebook'),'/notebooks/ordinary-notebook');
""".replace("MODULE_URL", json.dumps(module.as_uri()))
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", script], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
