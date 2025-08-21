# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BFZaiko is a Flask-based inventory management system for bifocal (BF) lens manufacturing and general lens inventory operations. The system manages two distinct workflows:

- **BF System**: Specialized for bifocal lens manufacturing, order management, shipping, and inventory tracking
- **General System**: Common lens inventory operations, processing workflows, and master data management

## Development Commands

### Setup and Installation
```bash
# Install dependencies
pip install -r requirements.txt

# Create user (first time setup)
python create_user.py

# Run development server
python run.py
```

### Database Operations
The system uses SQL Server with pymssql driver. Database connection configuration is managed through environment variables in `.env` file.

## Architecture Overview

### Application Structure
- **Flask 3.0.0** web framework with Blueprint architecture for modular organization
- **SQLAlchemy 2.0.25** ORM with SQL Server backend
- **Bootstrap 5** for responsive UI design
- **WTForms** for form handling and CSRF protection

### Core Modules
- `app/routes.py` - Main BF system routes
- `app/routes_common.py` - General system operations (Blueprint: `common_bp`)
- `app/routes_gradation.py` - Gradation processing workflows
- `app/routes_master.py` - Master data management
- `app/routes_total.py` - Processing aggregation (Blueprint: `total_bp`)

### Data Models
- `app/models.py` - BF system models (BPRD_DAT, BFSP_MST, BRCP_DAT, etc.)
- `app/models_common.py` - General system models (CPRD_DAT, CZTR_MST, etc.)
- `app/models_master.py` - Master data models (PRD_MST, KBN_MST)
- `app/models_total.py` - Aggregation and processing models

### Key Database Tables

#### BF System Tables
- **BPRD_DAT**: BF manufacturing data (aggregated)
- **BPRD_MEI**: BF manufacturing details (split management)
- **BRCP_DAT**: BF order receipt data
- **BSHK_DAT**: BF shipping data
- **BFSP_MST**: BF specification master

#### General System Tables
- **CPRD_DAT**: Common product data
- **CSHK_DAT**: Common shipping data
- **PRD_MST**: Product master
- **CZTR_MST**: Consignment destination master

### Business Logic Structure

#### BF vs General System Separation
The navigation and business logic clearly separate BF-specific operations from general operations. This is critical for understanding data flow:

```
BF Section (Bifocal-specific):
├── BF Order Input (BRCP_DAT)
├── BF Inventory Search (BPRD_DAT with Get_Zaiko_Qty_BF)
├── BF Shipping Management (BSHK_DAT)
└── BF Manufacturing Details (BPRD_MEI ↔ BPRD_DAT sync)

General Section (Common operations):
├── General Shipping (CSHK_DAT with categories: shipping/processing/loss)
├── General Inventory (CPRD_DAT)
├── Master Management (PRD_MST, KBN_MST, CZTR_MST)
└── Data Import/Export (CSV/Excel processing)
```

### Important Business Rules

#### BF Manufacturing Detail Management
- **BPRD_MEI** (manufacturing details) and **BPRD_DAT** (aggregated manufacturing data) maintain automatic synchronization
- Split management: Multiple BPRD_MEI records can aggregate to a single BPRD_DAT record
- Any changes to BPRD_MEI automatically update the corresponding BPRD_DAT quantities

#### Inventory Calculation Functions
- `Get_Zaiko_Qty_BF(BPDD_ID)`: BF-specific inventory calculation
- `Get_ODR_ZAN_Qty_BF(BRCP_ID)`: BF order remaining quantity calculation
- `Get_CPRD_ZAN_Qty()`: General system remaining quantity calculation

### Processing Workflows
- **Non-coat (NC)**: Standard manufacturing process (PROC = 0)
- **Hard-coat (HC)**: Advanced coating process (PROC = 1)
- **Gradation**: Specialized gradation processing with external contractors

### Configuration and Constants
- `app/constants.py`: Centralized constant definitions including database flags, status codes, and business rule constants
- Environment variables managed through `.env` file for database connection and application settings
- `app/database.py`: Database connection and session management with Japanese collation support

### Authentication and Session Management
- Session-based authentication with `@login_required` decorator
- Blueprint-based route organization for different system sections
- CSRF protection enabled through Flask-WTF

### File Processing
- **CSV Import**: Non-coat manufacturing data import (`app/import_csv.py`)
- **Excel Processing**: Hard-coat instruction sheet generation and reading (`app/import_excel.py`, `app/export_excel.py`)
- **PDF Export**: Inventory reports and shipping documentation (`app/export_pdf.py`)
- **Barcode**: Barcode generation and scanning functionality for inventory tracking

### Error Handling and Logging
- Centralized error handling with automatic transaction rollback
- Custom logging utilities in `app/logger_utils.py`
- Japanese text processing support for SQL Server UTF-8 handling

## Development Notes

### Database Collation
All string fields use `'Japanese_CI_AS'` collation for proper Japanese text handling in SQL Server.

### Transaction Management
Most database operations use explicit transaction management with automatic rollback on errors. Always wrap multi-table operations in try-catch blocks.

### Template Structure
- `base.html`: Main navigation template showing BF vs General system separation
- Modular template organization by functional area (common/, gradation/, master/, etc.)

### Blueprint Registration
The application uses Flask Blueprints for modular organization:
- Authentication: `auth` blueprint
- Common operations: `common_bp` blueprint  
- Processing totals: `total_bp` blueprint

Understanding the dual BF/General system architecture is essential for making appropriate modifications to either workflow without affecting the other.