import * as React from "react"
import { cn } from "../../lib/utils"

export const Checkbox = React.forwardRef<HTMLInputElement, React.InputHTMLAttributes<HTMLInputElement>>(({ className, checked, onChange, ...props }, ref) => (
  <input ref={ref} type="checkbox" checked={checked} onChange={onChange} className={cn("h-4 w-4 shrink-0 cursor-pointer rounded-[4px] border border-input bg-background accent-primary transition focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50", className)} {...props} />
))
Checkbox.displayName = "Checkbox"
