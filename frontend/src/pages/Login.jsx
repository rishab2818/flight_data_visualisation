import React, {useState} from 'react'
import api from '../api'
import { useAuth } from '../store'

export default function Login(){
  const [username, setU] = useState('admin')
  const [password, setP] = useState('admin')
  const [err, setErr] = useState('')
  const { login } = useAuth()
  async function onLogin(e){
    e.preventDefault(); setErr('')
    try{
      const res = await api.post('/auth/login', {username, password})
      login(res.data.access_token, res.data.user); location.href='/'
    }catch(e){ setErr(e?.response?.data?.detail || 'login failed') }
  }
  return (<div className="container">
    <div className="max-w-md mx-auto mt-20 card">
      <h2 className="text-lg font-semibold mb-3">Login</h2>
      <form onSubmit={onLogin} className="space-y-3">
        <div><label className="label">Username</label><input className="input w-full" value={username} onChange={e=>setU(e.target.value)} /></div>
        <div><label className="label">Password</label><input type="password" className="input w-full" value={password} onChange={e=>setP(e.target.value)} /></div>
        <button className="btn w-full" type="submit">Sign in</button>
        {err && <div className="text-red-600 text-sm">{err}</div>}
      </form>
    </div>
  </div>)
}
