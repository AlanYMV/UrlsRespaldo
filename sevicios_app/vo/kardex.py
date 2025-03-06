class Kardex:
    def __init__(self,item,location,date_stamp,user_tamp,quantity,before_on_hand_qty,after_on_hand_qty,before_in_transit_qty,after_in_transit_qty,before_alloc_qty,after_alloc_qty):
        self.item = item
        self.location = location
        self.date_stamp = date_stamp
        self.user_tamp = user_tamp
        self.quantity = quantity
        self.before_on_hand_qty = before_on_hand_qty
        self.after_on_hand_qty = after_on_hand_qty
        self.before_in_transit_qty = before_in_transit_qty
        self.after_in_transit_qty = after_in_transit_qty
        self.before_alloc_qty = before_alloc_qty
        self.after_alloc_qty = after_alloc_qty