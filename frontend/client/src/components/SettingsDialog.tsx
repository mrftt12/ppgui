import {
    Dialog,
    DialogContent,
    DialogHeader,
    DialogTitle,
} from "@/components/ui/dialog";
import {
    Tabs,
    TabsContent,
    TabsList,
    TabsTrigger,
} from "@/components/ui/tabs";
import { Label } from "@/components/ui/label";
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";

export interface Settings {
    frequency: "50" | "60";
    units: "metric" | "imperial";
    paletteView: "grid" | "spatial";
}

interface SettingsDialogProps {
    open: boolean;
    onOpenChange: (open: boolean) => void;
    settings: Settings;
    onSettingsChange: (settings: Settings) => void;
}

export function SettingsDialog({
    open,
    onOpenChange,
    settings,
    onSettingsChange,
}: SettingsDialogProps) {
    const updateSetting = <K extends keyof Settings>(key: K, value: Settings[K]) => {
        onSettingsChange({ ...settings, [key]: value });
    };

    return (
        <Dialog open={open} onOpenChange={onOpenChange}>
            <DialogContent className="sm:max-w-[425px]">
                <DialogHeader>
                    <DialogTitle>Settings</DialogTitle>
                </DialogHeader>
                <Tabs defaultValue="general" className="w-full">
                    <TabsList className="w-full grid grid-cols-2">
                        <TabsTrigger value="general">General</TabsTrigger>
                        <TabsTrigger value="display" disabled>Display (Soon)</TabsTrigger>
                    </TabsList>
                    <TabsContent value="general" className="space-y-4 py-4">
                        <div className="space-y-4">
                            <div className="grid grid-cols-4 items-center gap-4">
                                <Label htmlFor="frequency" className="text-right">
                                    Frequency
                                </Label>
                                <div className="col-span-3">
                                    <Select
                                        value={settings.frequency}
                                        onValueChange={(val) => updateSetting("frequency", val as "50" | "60")}
                                    >
                                        <SelectTrigger id="frequency">
                                            <SelectValue placeholder="Select frequency" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="50">50 Hz</SelectItem>
                                            <SelectItem value="60">60 Hz</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>
                            </div>
                            <div className="grid grid-cols-4 items-center gap-4">
                                <Label htmlFor="units" className="text-right">
                                    Units
                                </Label>
                                <div className="col-span-3">
                                    <Select
                                        value={settings.units}
                                        onValueChange={(val) => updateSetting("units", val as "metric" | "imperial")}
                                    >
                                        <SelectTrigger id="units">
                                            <SelectValue placeholder="Select units" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="metric">Metric</SelectItem>
                                            <SelectItem value="imperial">Imperial</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>
                            </div>
                        </div>

                        <div className="space-y-3 pt-4 border-t">
                            <Label className="text-base">Element Palette View</Label>
                            <RadioGroup
                                value={settings.paletteView}
                                onValueChange={(val) => updateSetting("paletteView", val as "grid" | "spatial")}
                                className="grid grid-cols-2 gap-4"
                            >
                                <div>
                                    <RadioGroupItem value="grid" id="grid" className="peer sr-only" />
                                    <Label
                                        htmlFor="grid"
                                        className="flex flex-col items-center justify-between rounded-md border-2 border-muted bg-popover p-4 hover:bg-accent hover:text-accent-foreground peer-data-[state=checked]:border-primary [&:has([data-state=checked])]:border-primary cursor-pointer"
                                    >
                                        <span className="text-sm font-medium">Grid View</span>
                                    </Label>
                                </div>
                                <div>
                                    <RadioGroupItem value="spatial" id="spatial" className="peer sr-only" />
                                    <Label
                                        htmlFor="spatial"
                                        className="flex flex-col items-center justify-between rounded-md border-2 border-muted bg-popover p-4 hover:bg-accent hover:text-accent-foreground peer-data-[state=checked]:border-primary [&:has([data-state=checked])]:border-primary cursor-pointer"
                                    >
                                        <span className="text-sm font-medium">Spatial Map</span>
                                    </Label>
                                </div>
                            </RadioGroup>
                        </div>
                    </TabsContent>
                </Tabs>
            </DialogContent>
        </Dialog>
    );
}
