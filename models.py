from pydantic import BaseModel, Field
# from datetime import datetime
from typing import Any, Dict, Optional, List
# from bson import ObjectId
from dataclasses import dataclass
from fastapi import Depends, Header


class Organisation(BaseModel):
    id: Optional[int]
    name: Optional[str]

class Devise(BaseModel):
    name: Optional[str]
    deviceType: Optional[str]

class Location(BaseModel):
    latitude: Optional[str]
    longitude: Optional[str]
    address: Optional[str]

class Vehicle(BaseModel):
    id: Optional[str]
    regNumber: Optional[str]

class User(BaseModel):
    id: Optional[int]
    name: Optional[str]
# class RejectReason(BaseModel):
#     id: int
#     name: str

class PaymentStatus(BaseModel):
    id: Optional[int]
    name: Optional[str]


class PaymentMedium(BaseModel):
    id: Optional[int]
    name: Optional[str]


class Offence(BaseModel):
    id: Optional[str]
    name: Optional[str]
    code: Optional[str]
    fine_amount: Optional[float]
    fine_point: Optional[int]


# establishing Instance log object
class Instance(BaseModel):
    # id: str = Field(..., alias="_id")
    # createdAt: Optional[datetime]
    imageUrl: str
    organisation: Organisation
    devise: Devise
    location: Location
    vehicle: Vehicle
    make: str
    model: str
    color: str
    isTaxi: bool
    isAnonymous: bool
    hasViolation: bool
    state: str
    # createdAt: datetime = datetime.utcnow()



# establishing violation scheduler object
class ViolationScheduler(BaseModel):
    id: str
    transactionId: Optional[str]
    androidId: Optional[str]
    approvalMode: Optional[int]
    devise: Devise
    handleType: Optional[str]
    instanceLogId: Optional[str]
    isConvertedToOffense: Optional[bool]
    isProcessed: Optional[bool]
    location: Location
    offenseCode: Optional[str]
    organisation: Organisation
    vehicle: Vehicle


class ViolationSchedulerUpdate(BaseModel):
    violation_scheduler_id: str
    approval_mode: int
    reviewer: Optional[str]
    user: User
    reason: List[str]


class OffencePaymentInstance(BaseModel):
    # id: Optional[str]
    # created_date: Optional[str]
    vehicle_offence_id: Optional[str]
    organisation: Organisation
    vehicle: Vehicle
    offence: Offence
    # amount_payable: Optional[float]
    invoice_id: Optional[str]
    payment_status: PaymentStatus


class OffencePaymentUpdate(BaseModel):
    invoice_id: Optional[str]
    payment_status: PaymentStatus
    payment_medium: PaymentMedium
    payment_partner: Optional[str]


class UpdateOffencePayment(BaseModel):
    invoice_id: Optional[str]
    payment_status: PaymentStatus
    payment_medium: PaymentMedium
    payment_partner: Optional[str]
def get_OrganisationId(OrganisationId: int = Header(...)):
    return OrganisationId


class ViolationSchedulerFunctionArguments(BaseModel):
    device_name: Optional[str] = None
    device_type: Optional[str] = None
    approvalmode: Optional[str] = None
    name: Optional[str] = None
    year: Optional[int] = None
    quarter: Optional[int] = None
    month: Optional[str] = None
    weeknumber: Optional[int] = None
    weekday: Optional[str] = None
    day: Optional[int] = None
    vehicle_id: Optional[str] = None
    vehicle_regnumber: str = None
    # username: Optional[str] = None
    # user_id: Optional[str] = None
    # offender: Optional[str] = None
    # offensecode: Optional[str] = None
    # reviewer: Optional[str] = None
    created_at_start: Optional[str] = None
    created_at_end: Optional[str] = None
    drill_down: Optional[str] = None


class OffencePaymentFunctionArguments(BaseModel):
    organisation_id: Optional[int] = 0
    updatedat_year: Optional[int] = 0
    updatedat_quarter: Optional[int] = 0
    updatedat_month: Optional[str] = 'None'
    updatedat_week: Optional[int] = 0
    updatedat_weekday: Optional[str] = 'None'
    updatedat_day: Optional[int] = 0
    vehicle_id: Optional[str] = 'None'
    vehicle_regnumber: str = 'None'
    offensename: Optional[str] = 'None'
    created_at_start: Optional[str] = 'None'
    created_at_end: Optional[str] = 'None'
    period: Optional[str] = "year"



class Installation(BaseModel):
    id: Optional[str]
    name: Optional[str]
class InstallationHealthData(BaseModel):
    installation: Installation
    location: Location
    organisation: Organisation
    isOnline: bool

class ServiceHealthData(BaseModel):
    installation_name: Optional[str]
    service_name: Optional[str]
    location: Location
    organisation: Organisation
    isOnline: bool


class FusionChartData(BaseModel):
    chart: dict
    categories: list
    dataset: list


class DeviseName(BaseModel):
    name: Optional[str]

class LocationFloat(BaseModel):
    latitude: Optional[float]
    longitude: Optional[float]
    address: Optional[str]


class CreateWatchlistHits(BaseModel):
    devise: DeviseName
    location: LocationFloat
    organisation: Organisation
    vehicle: Vehicle







# class ViolationSchedulerFunctionArguments(BaseModel):
#     organisation_id: Optional[int]
#     device_name: Optional[str]
#     device_type: Optional[str]
#     approvalmode: Optional[str]
#     name: Optional[str]
#     year: Optional[int]
#     quarter: Optional[int]
#     month: Optional[str]
#     weeknumber: Optional[int]
#     weekday: Optional[str]
#     day: Optional[int]
#     vehicle_id: Optional[str]
#     vehicle_regnumber: Optional[str]
#     username: Optional[str]
#     user_id: Optional[str]
#     offender: Optional[str]
#     offensecode: Optional[str]
#     reviewer: Optional[str]
#     created_at_start: Optional[str]
#     created_at_end: Optional[str]

# vehicle_offence_id: Optional[str]
# organisation_id: Optional[str]
# organisation_name: Optional[str]
# vehicle_id: Optional[str]
# reg_number: Optional[str]
# offense_id: Optional[str]
# offence_name: Optional[str]
# offence_code: Optional[str]
# fine_amount: Optional[float]
# fine_point: Optional[int]
# payment_status_id: Optional[str]
# payment_status_name: Optional[str]
# payment_medium_id: Optional[str]
# payment_medium_name: Optional[str]
# invoice_id: Optional[str]
# payment_partner: Optional[str]

