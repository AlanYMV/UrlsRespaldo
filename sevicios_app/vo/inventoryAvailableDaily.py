class InventoryAvailableDaily():
    
    def __init__(self, item, on_hand, in_transit, allocated,suspense,requested,quantity,real_available,date_time):
        self.item=item
        self.on_hand=on_hand
        self.in_transit=in_transit
        self.allocated=allocated
        self.suspense=suspense
        self.requested=requested
        self.quantity=quantity
        self.real_available=real_available
        self.date_time=date_time