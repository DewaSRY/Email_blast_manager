import type { Metadata } from "next";
import "./globals.css";
export const metadata: Metadata = { title: "Bulk Email Validator", description: "Validate permission-based email lists with syntax and DNS checks." };
export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) { return <html lang="en"><body>{children}</body></html>; }
