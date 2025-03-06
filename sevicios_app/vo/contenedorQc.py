class ContenedorQc():
    
    def __init__(self,container_id,weight,user_def1,total_freight_charge,base_freight_charge,freight_discount,accessorial_charge,qc_assignment_id,qc_status,fecha):
        self.container_id = container_id
        self.weight = weight
        self.user_def1 = user_def1
        self.total_freight_charge = total_freight_charge
        self.base_freight_charge = base_freight_charge
        self.freight_discount = freight_discount
        self.accessorial_charge = accessorial_charge
        self.qc_assignment_id = qc_assignment_id
        self.qc_status = qc_status
        self.fecha = fecha