import create from 'zustand'
export const useAuth = create((set)=> ({
  token: localStorage.getItem('token') || '',
  user: JSON.parse(localStorage.getItem('user') || 'null'),
  login: (token,user)=>{ localStorage.setItem('token', token); localStorage.setItem('user', JSON.stringify(user)); set({token,user}) },
  logout: ()=>{ localStorage.removeItem('token'); localStorage.removeItem('user'); set({token:'', user:null}) }
}))
