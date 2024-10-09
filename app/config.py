import os

class Settings:
    # Class-level attributes for settings
    RedisUrl = 'redis://localhost:6379/0'  # Example Redis URL
    volatility_trained_models = './app/volatility/trained_models/'  # Example directory for trained models
    price_trained_models = './app/price/trained_models/'


    @classmethod
    def get(cls, key):
        """
        Retrieve the value of a setting by key.

        Parameters
        ----------
        key : str
            The name of the setting to retrieve.

        Returns
        -------
        value : str
            The value of the requested setting.
        """
        return getattr(cls, key, None)

    @classmethod
    def set(cls, key, value):
        """
        Set the value of a setting.

        Parameters
        ----------
        key : str
            The name of the setting to update.
        value : str
            The new value for the setting.
        """
        setattr(cls, key, value)

    def __repr__(self):
        """
        String representation of the settings for debugging purposes.
        """
        return (f"trained_models={self.trained_models}")

# Create an instance of Settings to use in imports
settings = Settings()