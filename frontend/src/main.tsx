import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import Orders from './Orders.tsx'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <Orders />
  </StrictMode>,
)
