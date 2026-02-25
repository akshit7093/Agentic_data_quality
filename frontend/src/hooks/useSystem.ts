import { useQuery } from '@tanstack/react-query';
import { systemApi } from '@/services/api';
import { useStore } from '@/store';

export const useSystemHealth = () => {
  return useQuery({
    queryKey: ['system', 'health'],
    queryFn: systemApi.health,
    refetchInterval: 30000, // Refetch every 30 seconds
  });
};

export const useLLMHealth = () => {
  const setLLMHealth = useStore((state) => state.setLLMHealth);
  
  return useQuery({
    queryKey: ['system', 'llm-health'],
    queryFn: async () => {
      const data = await systemApi.llmHealth();
      setLLMHealth(data);
      return data;
    },
    refetchInterval: 30000,
    retry: 2,
  });
};

export const useSupportedSources = () => {
  return useQuery({
    queryKey: ['system', 'supported-sources'],
    queryFn: systemApi.getSupportedSources,
  });
};
