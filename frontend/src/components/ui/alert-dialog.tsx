import * as DialogPrimitive from "@radix-ui/react-dialog"
import type { ReactNode } from "react"
import { Button } from "./button"

export function AlertDialog({ open, onOpenChange, title, description, onConfirm, children }: { open: boolean; onOpenChange: (open: boolean) => void; title: string; description: string; onConfirm: () => void; children: ReactNode }) {
  return <DialogPrimitive.Root open={open} onOpenChange={onOpenChange}><DialogPrimitive.Trigger asChild>{children}</DialogPrimitive.Trigger><DialogPrimitive.Portal><DialogPrimitive.Overlay className="dialog-overlay" /><DialogPrimitive.Content className="dialog-content"><DialogPrimitive.Title className="dialog-title">{title}</DialogPrimitive.Title><DialogPrimitive.Description className="dialog-description">{description}</DialogPrimitive.Description><div className="dialog-actions"><DialogPrimitive.Close asChild><Button variant="outline">取消</Button></DialogPrimitive.Close><Button variant="destructive" onClick={() => { onConfirm(); onOpenChange(false) }}>确认停止</Button></div></DialogPrimitive.Content></DialogPrimitive.Portal></DialogPrimitive.Root>
}
