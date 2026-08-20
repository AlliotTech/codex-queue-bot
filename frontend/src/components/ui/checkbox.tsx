import * as React from "react"
export function Checkbox({ checked, onChange, ...props }: React.InputHTMLAttributes<HTMLInputElement>) { return <input type="checkbox" checked={checked} onChange={onChange} className="h-4 w-4 rounded border-input accent-primary" {...props} /> }
