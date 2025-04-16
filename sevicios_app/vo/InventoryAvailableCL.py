class InventoryAvailableCL():
    
    def __init__(self, item, item_desc,available,on_hand,allocated, in_transit,suspense,family,subfamily,subsubfamily,date_time):
        self.item=item
        self.item_desc=item_desc
        self.available=available
        self.on_hand=on_hand        
        self.allocated=allocated
        self.in_transit=in_transit
        self.suspense=suspense
        self.family=family
        self.subfamily=subfamily
        self.subsubfamily=subsubfamily
        self.date_time=date_time