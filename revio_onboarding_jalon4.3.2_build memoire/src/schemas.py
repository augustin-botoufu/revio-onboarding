"""Revio import target schemas.

Each schema describes the columns expected in the final Revio import CSV,
the mandatory fields, allowed values, and expected formats.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FieldSpec:
    name: str
    mandatory: bool = False
    description: str = ""
    allowed_values: Optional[list] = None
    format_hint: Optional[str] = None


# ---------- VEHICLE ----------
VEHICLE_FIELDS = [
    FieldSpec("registrationPlate", mandatory=True, format_hint="AB-123-CD"),
    FieldSpec("usage", mandatory=True, allowed_values=["utility", "service", "private"]),
    FieldSpec("parcEntryAt", format_hint="YYYY/MM/DD"),
    FieldSpec("brand"),
    FieldSpec("model"),
    FieldSpec("variant"),
    FieldSpec("motorisation", allowed_values=["diesel", "gas", "hybrid", "electric"]),
    FieldSpec("electricAutonomy", format_hint="km, integer"),
    FieldSpec("electricEnginePower", format_hint="kW, integer"),
    FieldSpec("weight", format_hint="kg, integer"),
    FieldSpec("co2gKm", format_hint="g/km, integer"),
    # Note: in the real NAT01 import, there is an empty column between co2gKm and registrationIssueCountryCode.
    # We keep it to stay faithful to the template the user showed.
    FieldSpec("", description="[empty column between co2gKm and registrationIssueCountryCode]"),
    FieldSpec("registrationIssueCountryCode", mandatory=True, format_hint="ISO 2-letter, e.g. FR"),
    FieldSpec("registrationIssueDate", format_hint="YYYY/MM/DD"),
    FieldSpec("registrationVin", format_hint="17 chars VIN"),
    FieldSpec("registrationFiscalPower", format_hint="integer"),
    FieldSpec("imageUrl"),
]

# ---------- DRIVER ----------
DRIVER_FIELDS = [
    FieldSpec("firstName", mandatory=True),
    FieldSpec("lastName", mandatory=True),
    FieldSpec("civility", allowed_values=["1", "2"], description="1=M, 2=F"),
    FieldSpec("birthDate", format_hint="YYYY/MM/DD"),
    FieldSpec("birthCity"),
    FieldSpec("emailPro", mandatory=True),
    FieldSpec("emailPerso"),
    FieldSpec("phone", format_hint="digits only, no + (33...)"),
    FieldSpec("street"),
    FieldSpec("city"),
    FieldSpec("postalCode"),
    FieldSpec("countryCode", format_hint="ISO 2-letter, e.g. FR"),
    FieldSpec("seniority", allowed_values=["employee", "manager", "leadership", "executive"]),
    FieldSpec(
        "professionalStatus",
        mandatory=True,
        allowed_values=["internal", "external"],
    ),
    FieldSpec("licenseNumber"),
    FieldSpec("licenseIssueCountryCode", format_hint="ISO 2-letter, e.g. FR"),
    FieldSpec("licenseIssueLocation"),
    FieldSpec("licenseIssueDate", format_hint="YYYY/MM/DD"),
    FieldSpec("licenseExpiryDate", format_hint="YYYY/MM/DD"),
    FieldSpec("assignPlate"),
    FieldSpec("registrationIssueCountryCode", format_hint="ISO 2-letter, e.g. FR"),
    FieldSpec("assignFrom", format_hint="YYYY/MM/DD"),
    FieldSpec("assignTo", format_hint="YYYY/MM/DD"),
    FieldSpec("companyAnalyticalCode", description="Agency code (e.g. NAT01, RENNES)"),
    FieldSpec("locationId", description="hidden/internal, leave empty"),
]

# ---------- CONTRACT ----------
CONTRACT_FIELDS = [
    FieldSpec("plate", mandatory=True),
    FieldSpec("plateCountry", mandatory=True, format_hint="ISO 2-letter"),
    FieldSpec("partnerId", mandatory=True, description="Revio UUID of the lessor"),
    FieldSpec("number", mandatory=True, description="Contract number"),
    FieldSpec("isHT", allowed_values=["TRUE", "FALSE"]),
    FieldSpec("durationMonths", format_hint="integer"),
    FieldSpec("startDate", mandatory=True, format_hint="YYYY/MM/DD"),
    FieldSpec("endDate", mandatory=True, format_hint="YYYY/MM/DD"),
    FieldSpec("contractedMileage", format_hint="integer km"),
    FieldSpec("maxMileage", format_hint="integer km"),
    FieldSpec("extraKmPrice", format_hint="decimal"),
    FieldSpec("vehicleValue", format_hint="decimal (mandatory for VP)"),
    FieldSpec("batteryValue", format_hint="decimal"),
    FieldSpec("civilLiabilityPrice"),
    FieldSpec("legalProtectionPrice"),
    FieldSpec("theftFireAndGlassPrice"),
    FieldSpec("allRisksPrice"),
    FieldSpec("financialLossPrice"),
    FieldSpec("maintenancePrice"),
    FieldSpec("replacementVehiclePrice"),
    FieldSpec("tiresPrice"),
    FieldSpec("gasCardPrice"),
    FieldSpec("tollCardPrice"),
    FieldSpec("totalPrice"),
    FieldSpec("civilLiabilityEnabled", allowed_values=["TRUE", "FALSE"]),
    FieldSpec("legalProtectionEnabled", allowed_values=["TRUE", "FALSE"]),
    FieldSpec("theftFireAndGlassEnabled", allowed_values=["TRUE", "FALSE"]),
    FieldSpec("allRisksEnabled", allowed_values=["TRUE", "FALSE"]),
    FieldSpec("financialLossEnabled", allowed_values=["TRUE", "FALSE"]),
    FieldSpec("maintenanceEnabled", allowed_values=["TRUE", "FALSE"]),
    FieldSpec("maintenanceNetwork", allowed_values=["standard", "specialist", "any"]),
    FieldSpec("replacementVehicleEnabled", allowed_values=["TRUE", "FALSE"]),
    FieldSpec("replacementVehicleCategory", description="e.g. C"),
    FieldSpec("replacementVehicleDurationBreakdown"),
    FieldSpec("replacementVehicleDurationAccident"),
    FieldSpec("replacementVehicleDurationTheft"),
    FieldSpec("tiresEnabled", allowed_values=["TRUE", "FALSE"]),
    FieldSpec("tiresType", allowed_values=["winter", "4seasons", "standard"]),
    FieldSpec("tiresAmount", format_hint="integer"),
    FieldSpec("tiresAmountUsed", format_hint="integer"),
    FieldSpec("tiresNetwork", allowed_values=["standard", "specialist", "any"]),
    FieldSpec("gasCardEnabled", allowed_values=["TRUE", "FALSE"]),
    FieldSpec("gasCardPartnerId"),
    FieldSpec("tollCardEnabled", allowed_values=["TRUE", "FALSE"]),
    FieldSpec("tollCardPartnerId"),
]

# ---------- ASSET (fuel cards, toll tags) ----------
ASSET_FIELDS = [
    FieldSpec("partnerId", mandatory=True, description="Revio UUID of the card/tag issuer"),
    FieldSpec("kind", mandatory=True, allowed_values=["fuel_card", "toll_tag"]),
    FieldSpec("identifier"),
    FieldSpec("expireAt", format_hint="YYYY/MM/DD"),
    FieldSpec("assignEmail", description="Forbidden if assignPlate is set"),
    FieldSpec("assignPlate", description="Forbidden if assignEmail is set"),
    FieldSpec(
        "registrationIssueCountryCode",
        description="Mandatory if assignPlate is set",
        format_hint="ISO 2-letter",
    ),
    FieldSpec("assignFrom", description="Mandatory if assignEmail or assignPlate", format_hint="YYYY/MM/DD"),
    FieldSpec("assignTo", format_hint="YYYY/MM/DD"),
]


SCHEMAS = {
    "vehicle": VEHICLE_FIELDS,
    "driver": DRIVER_FIELDS,
    "contract": CONTRACT_FIELDS,
    "asset": ASSET_FIELDS,
}


def header_for(schema_name: str) -> list[str]:
    """Return the ordered list of column names for a schema."""
    return [f.name for f in SCHEMAS[schema_name]]


def mandatory_fields_for(schema_name: str) -> list[str]:
    return [f.name for f in SCHEMAS[schema_name] if f.mandatory]
