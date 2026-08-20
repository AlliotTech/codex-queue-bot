import * as React from "react"
import { cn } from "../../lib/utils"

export function Button({ className, variant = "default", size = "default", ...props }: React.ButtonHTMLAttributes<HTMLButtonElement> & { variant?: "default" | "outline" | "ghost" | "destructive"; size?: "default" | "sm" }) {
  return <button className={cn("inline-flex items-center justify-center rounded-md text-sm font-medium transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-50", variant === "default" && "bg-primary text-primary-foreground hover:bg-primary/90", variant === "outline" && "border border-input bg-background hover:bg-accent hover:text-accent-foreground", variant === "ghost" && "hover:bg-accent hover:text-accent-foreground", variant === "destructive" && "bg-destructive text-destructive-foreground hover:bg-destructive/90", size === "default" ? "h-10 px-4" : "h-8 px-3 text-xs", className)} {...props} />
}
