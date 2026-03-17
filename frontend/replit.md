# Power Flow Analyzer

## Overview

Power Flow Analyzer is a professional three-phase unbalanced load flow analysis application for electrical power systems. It provides a visual network editor where users can design power distribution networks by placing and connecting electrical components like transformers, buses, lines, loads, and generators. The application performs load flow analysis calculations on the network models to determine voltage, current, and power flow throughout the system.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture
- **Framework**: React 18 with TypeScript
- **Routing**: Wouter for lightweight client-side routing
- **State Management**: TanStack React Query for server state, local React state for UI
- **Styling**: Tailwind CSS with shadcn/ui component library (New York style)
- **Build Tool**: Vite with React plugin

The frontend follows a component-based architecture with:
- Main page component (`NetworkEditor`) serving as the application shell
- Reusable UI components from shadcn/ui in `client/src/components/ui/`
- Domain-specific components for power system modeling (ElementPalette, NetworkCanvas, PropertiesPanel, etc.)
- Custom hooks for mobile detection and toast notifications

### Backend Architecture
- **Runtime**: Node.js with Express 5
- **Language**: TypeScript compiled with tsx
- **API Style**: RESTful JSON API under `/api` prefix

The server provides:
- CRUD endpoints for network models (`/api/networks`)
- CRUD endpoints for equipment templates (`/api/equipment-templates`)
- Load flow analysis endpoint (`/api/networks/:id/analyze`)
- Static file serving for production builds
- Vite dev server integration for development

### Data Storage
- **ORM**: Drizzle ORM with PostgreSQL dialect
- **Schema Location**: `shared/schema.ts` contains all data models using Zod schemas
- **Current Implementation**: In-memory storage (`MemStorage` class in `server/storage.ts`)
- **Database Ready**: Drizzle configuration exists for PostgreSQL migration when database is provisioned

Key data models:
- `NetworkModel`: Contains network elements and connections
- `NetworkElement`: Power system components (buses, transformers, lines, loads, etc.)
- `Connection`: Links between network elements
- `EquipmentTemplate`: Predefined equipment specifications
- `LoadFlowResult`: Analysis output with per-element results

### Shared Code
The `shared/` directory contains code used by both frontend and backend:
- Type definitions and Zod schemas for all data models
- Element type enumerations
- Three-phase complex number structures for unbalanced analysis

### Build System
- Development: `tsx` runs TypeScript directly with Vite HMR
- Production: Custom build script using esbuild for server, Vite for client
- Output: `dist/` directory with `index.cjs` (server) and `public/` (client assets)

## External Dependencies

### Database
- **PostgreSQL**: Required for persistent storage (configured via `DATABASE_URL` environment variable)
- **Drizzle Kit**: For database schema migrations (`npm run db:push`)
- **connect-pg-simple**: Session storage for Express (available but not currently used)

### UI Component Library
- **shadcn/ui**: Pre-built accessible components based on Radix UI primitives
- **Radix UI**: Low-level UI primitives (dialogs, dropdowns, tabs, etc.)
- **Lucide React**: Icon library

### Development Tools
- **Replit Plugins**: Error overlay, cartographer, and dev banner for Replit environment
- **PostCSS/Autoprefixer**: CSS processing for Tailwind

### Notable NPM Packages
- `react-day-picker`: Calendar component
- `embla-carousel-react`: Carousel functionality
- `react-resizable-panels`: Resizable panel layout
- `vaul`: Drawer component
- `cmdk`: Command palette component
- `recharts`: Charting library (available for analysis visualization)