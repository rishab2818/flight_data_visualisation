import axios from 'axios'
import { useAuth } from './store'
const API_BASE = import.meta.env.VITE_API_BASE || ''
const api = axios.create({ baseURL: API_BASE })
api.interceptors.request.use((cfg)=>{ const { token } = useAuth.getState(); if(token) cfg.headers.Authorization = 'Bearer ' + token; return cfg })
export default api
