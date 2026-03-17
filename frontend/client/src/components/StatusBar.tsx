import { useQuery } from "@tanstack/react-query";
import { Wifi, WifiOff } from "lucide-react";
import { useEffect, useState } from "react";

export function StatusBar() {
    const [lastChecked, setLastChecked] = useState<Date>(new Date());

    const { isSuccess, isError, data } = useQuery({
        queryKey: ["/api/health"],
        queryFn: async () => {
            const res = await fetch("/api/health");
            if (!res.ok) throw new Error("API offline");
            return res.json();
        },
        refetchInterval: 30000, // Check every 30 seconds
        retry: false,
    });

    useEffect(() => {
        if (data || isError) {
            setLastChecked(new Date());
        }
    }, [data, isError]);

    return (
        <div className="h-6 border-t bg-card flex items-center px-3 justify-between text-[10px] text-muted-foreground select-none shrink-0">
            <div className="flex items-center gap-4">
                <div className="flex items-center gap-1.5">
                    {isSuccess ? (
                        <>
                            <div className="relative flex h-2 w-2">
                                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                                <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span>
                            </div>
                            <span className="text-green-600 font-medium">System Online</span>
                        </>
                    ) : (
                        <>
                            <div className="h-2 w-2 rounded-full bg-red-500"></div>
                            <span className="text-red-500 font-medium">System Offline</span>
                        </>
                    )}
                </div>

                <span className="hidden sm:inline">
                    Last checked: {lastChecked.toLocaleTimeString()}
                </span>
            </div>

            <div className="flex items-center gap-3">
                <span>v1.0.0</span>
                {isSuccess ? (
                    <Wifi className="h-3 w-3 text-muted-foreground" />
                ) : (
                    <WifiOff className="h-3 w-3 text-red-400" />
                )}
            </div>
        </div>
    );
}
