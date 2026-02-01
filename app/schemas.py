from pydantic import BaseModel, Field
from typing import Optional, List, Literal

# --- Search ---
class SearchResult(BaseModel):
    type: Literal["WORK","INSPECTION","ASSET","LOCATION"]
    id: int
    title: str
    subtitle: str = ""

# --- Inspection ---
class InspectionCreate(BaseModel):
    template_id: int
    category_id: int
    scope_type: Literal["ASSET","LOCATION"]
    asset_id: Optional[int] = None
    location_id: Optional[int] = None
    performed_at: Optional[str] = None
    overall_result: Literal["PASS","WARN","FAIL"]
    summary_note: Optional[str] = None
    auto_create_work: bool = True

class WorkAutoCreated(BaseModel):
    created: bool
    work_id: Optional[int] = None
    work_code: Optional[str] = None

# --- Work ---
WorkStatus = Literal["NEW","ASSIGNED","IN_PROGRESS","REVIEW","APPROVED","DONE","HOLD","REJECTED","CANCELED"]

class WorkCreate(BaseModel):
    source_type: Literal["INSPECTION","COMPLAINT","MAINTENANCE","OTHER"]
    source_id: Optional[int] = None
    category_id: int
    asset_id: Optional[int] = None
    location_id: Optional[int] = None
    title: str
    description: Optional[str] = None
    priority: int = Field(default=3, ge=1, le=5)
    is_emergency: bool = False
    assigned_to: Optional[int] = None
    due_at: Optional[str] = None

class WorkTransition(BaseModel):
    to_status: WorkStatus
    note: Optional[str] = None

# --- PR ---
PRStatus = Literal["DRAFT","REVIEW","APPROVED","REJECTED","CANCELED","ORDERED"]

class PRLine(BaseModel):
    item_id: Optional[int] = None
    item_name: str
    qty: float
    unit: str = "EA"
    target_price: Optional[float] = None
    spec_note: Optional[str] = None

class PRCreate(BaseModel):
    work_order_id: Optional[int] = None
    need_by: Optional[str] = None
    note: Optional[str] = None
    lines: List[PRLine] = []
