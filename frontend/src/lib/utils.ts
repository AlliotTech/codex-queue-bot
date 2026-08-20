import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatTime(value: string | null | undefined) {
  if (!value) return "—"
  return new Intl.DateTimeFormat(undefined, { dateStyle: "short", timeStyle: "medium" }).format(new Date(value))
}

export function countdown(value: string | null | undefined) {
  if (!value) return "—"
  const seconds = Math.max(0, Math.round((new Date(value).getTime() - Date.now()) / 1000))
  if (seconds <= 0) return "马上"
  if (seconds < 60) return `${seconds} 秒后`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes} 分钟后`
  return `${Math.floor(minutes / 60)} 小时后`
}
