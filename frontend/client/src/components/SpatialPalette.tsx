import { Map } from "lucide-react";

export function SpatialPalette() {
    return (
        <div className="flex flex-col items-center justify-center h-full p-4 text-center text-muted-foreground select-none">
            <div className="bg-muted/30 p-4 rounded-full mb-3">
                <Map className="h-8 w-8 opacity-50" />
            </div>
            <h3 className="font-medium text-foreground mb-1">Spatial Map</h3>
            <p className="text-xs max-w-[180px]">
                GIS View is currently under development. Switch back to Grid View to add elements.
            </p>
        </div>
    );
}
