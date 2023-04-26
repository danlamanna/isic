import factory
import factory.django

from isic.core.models import Collection, Image
from isic.core.models.isic_id import IsicId
from isic.factories import UserFactory
from isic.ingest.tests.factories import AccessionFactory


class IsicIdFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = IsicId

    id = factory.Faker("numerify", text="ISIC_#######")


class ImageFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Image

    isic_id = factory.SubFactory(IsicIdFactory)
    creator = factory.SubFactory(UserFactory)
    accession = factory.SubFactory(AccessionFactory)
    public = factory.Faker("boolean")


class CollectionFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Collection

    creator = factory.SubFactory(UserFactory)
    name = factory.Faker("sentence")
    description = factory.Faker("paragraph")
    public = factory.Faker("boolean")
    pinned = factory.Faker("boolean")
    locked = False
