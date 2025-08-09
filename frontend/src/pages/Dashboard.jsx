import React, { useEffect, useRef, useState } from 'react'
import api from '../api'
import { useAuth } from '../store'
import { LogOut, Upload, RefreshCw, Download, Play, Trash2 } from 'lucide-react'
import ThemeToggle from '../components/ThemeToggle'
import ConfirmDialog from '../components/ConfirmDialog'
import DataTable from '../components/DataTable'
import { Toaster, toast } from 'sonner'

export default function Dashboard(){
  const { token, user, logout } = useAuth()
  const [datasets, setDatasets] = useState([])
  const [file, setFile] = useState(null)
  const [name, setName] = useState('')
  const [job, setJob] = useState(null)
  const [logs, setLogs] = useState('')
  const [cols, setCols] = useState([])
  const [xcol, setX] = useState('')
  const [ycol, setY] = useState('')
  const [series, setSeries] = useState([]) // overlay series [{datasetId, yCol, label}]
  const wsRef = useRef(null)
  const [presets, setPresets] = useState([])
  const [presetName, setPresetName] = useState('')
  const [presetDesc, setPresetDesc] = useState('')

  async function refreshPresets(){
    try{
      const r = await api.get('/plots/presets')
      setPresets(r.data || [])
    }catch{}
  }
  useEffect(()=>{ refreshPresets() }, [])

  async function savePreset(){
    if(!xcol || !series.length){ toast.error('Set X and at least one series'); return }
    try{
      const config = { x_col: xcol, series, title: `${xcol} overlay` }
      await api.post('/plots/presets', { name: presetName || `Preset ${Date.now()}`, description: presetDesc, config })
      setPresetName(''); setPresetDesc(''); toast.success('Preset saved'); refreshPresets()
    }catch(e){ toast.error('Save failed') }
  }

  function applyPreset(p){
    const cfg = p.config || {}
    setX(cfg.x_col || ''); setSeries(cfg.series || [])
    toast.success('Preset applied')
  }

  async function deletePreset(id){
    try{ await api.delete(`/plots/presets/${id}`); refreshPresets(); toast.success('Preset deleted') }
    catch{ toast.error('Delete failed') }
  }

  const [confirm, setConfirm] = useState({open:false, id:null})

  useEffect(()=>{
    if(!token){ location.href='/login'; return }
    fetchDatasets()
  }, [token])

  async function fetchDatasets(){
    try{
      const res = await api.get('/datasets')
      setDatasets(res.data || [])
    }catch(e){ toast.error('Failed to load datasets') }
  }

  function handleFile(e){ setFile(e.target.files[0]) }

  async function upload(){
    if(!file){ toast.error('Pick a file'); return }
    try{
      const fd = new FormData(); fd.append('file', file); fd.append('name', name)
      const res = await api.post('/datasets/upload', fd)
      toast.success('Upload started')
      startWS(res.data.job_id, res.data.dataset_id)
    }catch(e){ toast.error('Upload failed') }
  }

  async function demo(){
    try{
      const res = await api.post('/datasets/demo_upload')
      toast.success('Demo started')
      startWS(res.data.job_id, res.data.dataset_id)
    }catch(e){ toast.error('Demo failed') }
  }

  function startWS(jobId, datasetId){
    if(wsRef.current){ try{ wsRef.current.close() }catch{} }
    setJob({id:jobId, dataset:datasetId, status:'pending', progress:0})
    setLogs(''); setCols([]); setX(''); setY(''); setSeries([])
    const proto = location.protocol === 'https:' ? 'wss' : 'ws'
    const WS_BASE = import.meta.env.VITE_WS_BASE || `${proto}://${location.host}`
    const wsUrl = `${WS_BASE}/jobs/ws/${jobId}`
    let pollTimer = null
    const beginPolling = () => {
      if (pollTimer) return
      pollTimer = setInterval(async () => {
        try {
          const r = await api.get(`/jobs/${jobId}`)
          const p = r.data || {}
          setLogs(p.logs || '')
          setJob(prev => ({ ...prev, status: p.status, progress: p.progress, message: p.message }))
          if (p.status === 'success') { clearInterval(pollTimer); pollTimer = null; loadColumns(datasetId); fetchDatasets(); toast.success('Parsing complete') }
          if (p.status === 'failed')  { clearInterval(pollTimer); pollTimer = null; toast.error(p.message || 'Parsing failed') }
        } catch {}
      }, 1000)
    }
    let ws
    try { ws = new WebSocket(wsUrl) } catch { beginPolling(); return }
    ws.onopen = () => console.log('ws open')
    ws.onmessage = (ev) => {
      try{
        const p = JSON.parse(ev.data)
        setLogs(p.logs||''); setJob(prev=>({...prev, status:p.status, progress:p.progress, message:p.message}))
        if(p.status === 'success'){ loadColumns(datasetId); fetchDatasets(); toast.success('Parsing complete') }
        if(p.status === 'failed'){ toast.error(p.message||'Parsing failed') }
      }catch(e){}
    }
    ws.onclose = ()=> { console.log('ws closed'); beginPolling() }
    wsRef.current = ws
  }

  async function loadColumns(datasetId){
    try{
      const res = await api.get(`/datasets/${datasetId}/columns`)
      const c = res.data.columns || []
      setCols(c); setX(c[0]||''); setY(c[1]||'')
      setSeries([{ datasetId, yCol: c[1] || '', label: '' }])
    }catch(e){ toast.error('Failed to load columns') }
  }

  function addSeries(){
    if(!job?.dataset){ toast.error('Select a dataset first'); return }
    setSeries(prev => [...prev, { datasetId: job.dataset, yCol: '', label: '' }])
  }
  function updateSeries(i, patch){ setSeries(prev => prev.map((s,idx)=> idx===i ? {...s, ...patch} : s)) }
  function removeSeries(i){ setSeries(prev => prev.filter((_,idx)=> idx!==i)) }

  function openPlot(){
    if(!job?.dataset || !xcol || !ycol){ toast.error('Select dataset and columns'); return }
    const token = useAuth.getState().token
    const url = `/plots/plot?dataset_id=${job.dataset}&x_col=${encodeURIComponent(xcol)}&y_col=${encodeURIComponent(ycol)}&token=${encodeURIComponent(token)}`
    window.open(url,'_blank')
  }

  async function openOverlay(){
    if(!xcol){ toast.error('Select X column'); return }
    const valid = series.filter(s => s.datasetId && s.yCol)
    if(!valid.length){ toast.error('Add at least one series'); return }
    try{
      const body = { title: `${xcol} overlay`, x_col: xcol, series: valid.map(s => ({ dataset_id: s.datasetId, y_col: s.yCol, label: s.label, filters: s.filters || [], computes: s.computes || [] })) }
      const res = await api.post('/plots/overlay', body, { responseType: 'blob' })
      const url = window.URL.createObjectURL(new Blob([res.data], { type: 'text/html' }))
      window.open(url, '_blank')
      setTimeout(()=> URL.revokeObjectURL(url), 30000)
    }catch(e){
      toast.error(e?.response?.data?.detail || 'Overlay failed')
    }
  }

  async function downloadFile(datasetId, type){
    try{
      const res = await api.get(`/datasets/${datasetId}/download_proxy`, { params: { file_type: type }, responseType: 'blob' })
      const url = window.URL.createObjectURL(new Blob([res.data]))
      const a = document.createElement('a'); a.href = url; a.download = type==='raw' ? 'raw.txt' : 'parsed.parquet'
      document.body.appendChild(a); a.click(); a.remove(); window.URL.revokeObjectURL(url)
    }catch(e){ toast.error('Download failed') }
  }

  async function deleteDataset(id){
    try{
      await api.delete(`/datasets/${id}`)
      toast.success('Dataset deleted')
      setConfirm({open:false, id:null})
      fetchDatasets()
    }catch(e){
      toast.error(e?.response?.data?.detail || 'Delete failed')
    }
  }

  const columnsDef = [
    { header:'Name', accessorKey:'name', cell: info => info.getValue() || info.row.original.id },
    { header:'Rows', accessorKey:'packet_count' },
    { header:'File', accessorKey:'original_filename' },
    { header:'Actions', cell: ({row})=> (
      <div className="flex gap-2">
        <button className="btn" onClick={()=>downloadFile(row.original.id,'raw')}><Download className="w-4 h-4"/> Raw</button>
        <button className="btn" onClick={()=>downloadFile(row.original.id,'parquet')}><Download className="w-4 h-4"/> Parquet</button>
        <button className="btn" onClick={()=>setConfirm({open:true, id:row.original.id})}><Trash2 className="w-4 h-4"/> Delete</button>
      </div>
    )}
  ]

  return (
    <div className="container">
      <Toaster richColors position="top-right" />
      <nav>
        <h1>Flight Data Platform</h1>
        <div className="flex items-center gap-2">
          <ThemeToggle/>
          <span className="badge">{user?.username}</span>
          <button className="btn" onClick={()=>logout()}><LogOut className="w-4 h-4"/> Logout</button>
        </div>
      </nav>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="card lg:col-span-2">
          <h3 className="text-base font-semibold mb-2">Upload & Parse</h3>
          <div className="flex flex-wrap gap-2 items-center">
            <input type="file" accept=".txt" onChange={handleFile} className="input" />
            <input className="input" placeholder="dataset name" value={name} onChange={e=>setName(e.target.value)} />
            <button className="btn" onClick={upload}><Upload className="w-4 h-4"/> Upload</button>
            <button className="btn" onClick={demo}><Play className="w-4 h-4"/> Demo</button>
          </div>
          <div className="mt-3">
            {job ? (<div className="space-y-2">
              <div className="text-sm">Job ID: <span className="badge">{job.id}</span></div>
              <div className="text-sm">Status: <span className="badge">{job.status}</span></div>
              <div className="progress"><div style={{width: (job.progress||0)+'%'}}></div></div>
              <div className="text-sm text-slate-600 dark:text-slate-300">Message: {job.message}</div>
              <div className="log mt-2">{logs}</div>
            </div>) : <div className="text-slate-500 text-sm">No active job</div>}
          </div>
        </div>

        <div className="card">
          <h3 className="text-base font-semibold mb-2">Plot Builder</h3>
          <div className="space-y-2">
            <div className="text-sm">Select dataset to load columns</div>
            <select className="input w-full" onChange={e=>{ const id=e.target.value; if(!id) return; setJob(prev=>({...prev, dataset:id})); loadColumns(id); }}>
              <option value="">-- select --</option>
              {datasets.map(d=> <option key={d.id} value={d.id}>{d.name||d.id} ({d.packet_count||0})</option>)}
            </select>
            <div className="flex gap-2">
              <select className="input w-full" value={xcol} onChange={e=>setX(e.target.value)}>
                <option value="">X Column</option>
                {cols.map(c=> <option key={c} value={c}>{c}</option>)}
              </select>
              <select className="input w-full" value={ycol} onChange={e=>setY(e.target.value)}>
                <option value="">Y Column</option>
                {cols.map(c=> <option key={c} value={c}>{c}</option>)}
              </select>
            </div>

            <div className="space-y-2">
              <div className="text-sm font-medium mt-2">Series (Y from any dataset)</div>
              {series.map((s, i)=> (
                <div key={i} className="flex gap-2 items-center">
                  <select className="input" value={s.datasetId} onChange={e=>updateSeries(i,{datasetId:e.target.value})}>
                    <option value="">dataset</option>
                    {datasets.map(d=> <option key={d.id} value={d.id}>{d.name||d.id}</option>)}
                  </select>
                  <select className="input" value={s.yCol} onChange={e=>updateSeries(i,{yCol:e.target.value})}>
                    <option value="">Y column</option>
                    {(s.datasetId===job?.dataset ? cols : (datasets.find(d=>d.id===s.datasetId)?.columns || [])).map(c=> (
                      <option key={c} value={c}>{c}</option>
                    ))}
                  </select>
                  <input className="input" placeholder="label (optional)" value={s.label||''} onChange={e=>updateSeries(i,{label:e.target.value})} />
                  <button className="btn" onClick={()=>removeSeries(i)}>Remove</button>
                </div>
              ))}
              <details className="border border-dashed border-slate-300 dark:border-slate-600 rounded p-2 mb-2">
                <summary className="cursor-pointer text-sm">Advanced: per-series filters & computed columns (optional)</summary>
                <div className="mt-2 text-xs text-slate-500">Leave empty if you don't need them.</div>
                {series.map((s, i)=> (
                  <div key={'fc-'+i} className="grid md:grid-cols-2 gap-2 rounded p-2 mb-2">
                    <textarea className="input" rows="2" placeholder='Filters JSON e.g. [{"col":"A","op":">=","value":10}]' value={JSON.stringify(s.filters||[])} onChange={e=>{ try{ updateSeries(i,{filters: JSON.parse(e.target.value)}) }catch{}}} />
                    <textarea className="input" rows="2" placeholder='Computes JSON e.g. [{"name":"ratio","expr":"Cl/Cd"}]' value={JSON.stringify(s.computes||[])} onChange={e=>{ try{ updateSeries(i,{computes: JSON.parse(e.target.value)}) }catch{}}} />
                  </div>
                ))}
              </details>
              <button className="btn w-full" onClick={addSeries}>+ Add line</button>
            </div>

            <button className="btn w-full" onClick={openPlot}>Open Plot (single)</button>
            <button className="btn w-full" onClick={openOverlay}>Open Overlay (multi)</button>
          </div>
        </div>
      </div>

      <div className="card mt-4">
        <div className="flex items-center justify-between">
          <h3 className="text-base font-semibold">Datasets</h3>
          <button className="btn" onClick={fetchDatasets}><RefreshCw className="w-4 h-4"/> Refresh</button>
        </div>
        <div className="mt-2">
          <DataTable columns={columnsDef} data={datasets} />
        </div>
      </div>


    <div className="card mt-4">
      <h3 className="text-base font-semibold mb-2">Presets</h3>
      <div className="grid md:grid-cols-2 gap-3">
        <div className="space-y-2">
          <input className="input w-full" placeholder="Preset name" value={presetName} onChange={e=>setPresetName(e.target.value)} />
          <input className="input w-full" placeholder="Description (optional)" value={presetDesc} onChange={e=>setPresetDesc(e.target.value)} />
          <button className="btn w-full" onClick={savePreset}>Save current overlay</button>
        </div>
        <div className="space-y-2">
          {presets.length === 0 ? <div className="text-sm text-slate-500">No presets yet</div> : (
            presets.map(p => (
              <div key={p.id} className="flex items-center gap-2 border border-slate-200 dark:border-slate-700 rounded-lg p-2">
                <div className="flex-1">
                  <div className="font-medium truncate">{p.name}</div>
                  <div className="text-xs text-slate-500">{p.description||''}</div>
                </div>
                <button className="btn" onClick={()=>applyPreset(p)}>Apply</button>
                <button className="btn" onClick={()=>deletePreset(p.id)}>Delete</button>
              </div>
            ))
          )}
        </div>
      </div>
    </div>

      <ConfirmDialog open={confirm.open} onOpenChange={(v)=>setConfirm({open:v, id: confirm.id})} title="Delete dataset?" description="This will permanently remove the dataset and files from disk." onConfirm={()=>deleteDataset(confirm.id)} />
    </div>
  )
}
