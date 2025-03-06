class KardexDownload:
    def __init__(self,item, transaction_type, description, location, container_id, reference_id, reference_type, work_type, date_stamp,user_stamp, quantity, before_sts, after_sts, before_on_hand_qty, after_on_hand_qty, before_in_transit_qty, after_in_transit_qty, before_suspense_qty, after_suspense_qty, before_alloc_qty, after_alloc_qty, direction):
        self.item = item  
        self.transaction_type = transaction_type 
        self.description = description
        self.location = location 
        self.container_id = container_id
        self.reference_id = reference_id 
        self.reference_type = reference_type 
        self.work_type = work_type
        self.date_stamp = date_stamp
        self.user_stamp = user_stamp 
        self.quantity = quantity 
        self.before_sts = before_sts 
        self.after_sts = after_sts 
        self.before_on_hand_qty = before_on_hand_qty 
        self.after_on_hand_qty = after_on_hand_qty 
        self.before_in_transit_qty = before_in_transit_qty 
        self.after_in_transit_qty = after_in_transit_qty 
        self.before_suspense_qty = before_suspense_qty 
        self.after_suspense_qty = after_suspense_qty 
        self.before_alloc_qty = before_alloc_qty
        self.after_alloc_qty = after_alloc_qty 
        self.direction = direction