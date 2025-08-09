import React, { useEffect, useState } from 'react'

export default function ThemeToggle(){
  const [dark, setDark] = useState(false)
  useEffect(()=>{
    const saved = localStorage.getItem('theme-dark') === '1'
    setDark(saved)
    document.documentElement.classList.toggle('dark', saved)
  },[])
  function toggle(){
    const v = !dark
    setDark(v)
    localStorage.setItem('theme-dark', v ? '1' : '0')
    document.documentElement.classList.toggle('dark', v)
  }
  return <button className="btn" onClick={toggle}>{dark ? 'Light' : 'Dark'} mode</button>
}
