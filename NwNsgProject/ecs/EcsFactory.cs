public class EcsFactory
{
    public static EcsAll createEcsAll(DenormalizedRecord denormalizedRecord)
    {
        EcsAll ecsAll = new EcsAll(denormalizedRecord);
        return ecsAll;
    }

}