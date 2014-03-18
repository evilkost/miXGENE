# -*- coding: utf-8 -*-

class MultiUploadField(dict):
    """
        Should contain UploadedFileWrapper instances
    """
    def to_dict(self, *args, **kwargs):
        return {
            "count": len(self),
            "files_info": [obj.to_dict(*args, **kwargs) for obj in self.values()],
            "previews": sorted([obj.orig_name for obj in self.values()])
        }
