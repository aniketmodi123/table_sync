from sqlalchemy import Column, Integer, String, DateTime, Float, func
from config import Base

class TowerConfig(Base):
    __tablename__ = 'tower_config'

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    site_id = Column(String(250), nullable=False, unique=True)
    site_name = Column(String(250), nullable=False)
    load_type = Column(String(5), nullable=False)
    tower_name = Column(String(200), nullable=False)
    email = Column(String(100), nullable=False)
    contact = Column(String(15), nullable=False)
    project = Column(String(250), nullable=False)
    gst_no = Column(String(25), nullable=False)
    pan_no = Column(String(15), nullable=True)
    address = Column(String(250), nullable=False)
    maintenance_charge = Column(Float(5), default=0.0, nullable=True)
    maintenance_gst_charge = Column(Float(5), default=0.0, nullable=True)
    other_charges = Column(Float(5), default=0.0, nullable=True)
    other_gst_charge = Column(Float(5), default=0.0, nullable=True)
    dev_logo = Column(String(250), nullable=True)
    created_date = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(String(150), nullable=False)
    edited_date = Column(DateTime, nullable=True, server_default=func.now())
    edited_by = Column(String(150), nullable=True)


class TariffConfig(Base):
    __tablename__ = 'tariff_config'

    id = Column(Integer, primary_key=True, index=True)
    site_id = Column(String(250), nullable=False)
    meter_ip = Column(String(40), unique=True, nullable=False)
    status = Column(String(10), nullable=False)
    eb_price = Column(Float)
    dg_price = Column(Float)
    eb_full_tariff = Column(String(100), nullable=False)
    dg_full_tariff = Column(String(100))
    timestamp = Column(DateTime, nullable=False)
    updated_by = Column(String(70),default="script")